package broker

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"uuid"

	"github.com/pipit-org/pipit-balancer/client"
	"github.com/pipit-org/pipit-balancer/node"
)

const originHeader = "Pipit-Origin-Id"

var errNoNodes = errors.New("sin nodos disponibles")

// Message es un fragmento de la respuesta de un nodo hacia un cliente.
// Una respuesta viaja como una secuencia de Messages con el mismo ID:
//
//  1. el primero trae Status y Header (Status != 0)
//  2. los siguientes traen Data (puede haber muchos: así funciona el streaming)
//  3. el último tiene Last = true
//
// Un solo Message puede cumplir varios roles (por ejemplo, un error trae los tres).
type Message struct {
	ID     string
	Status int
	Header http.Header
	Data   []byte
	Last   bool
}

// work es la respuesta pendiente de una petición.
type work struct {
	// canal por el que llegan los fragmentos de la respuesta
	ch chan *Message
	// se cierra cuando el cliente se desconecta
	gone <-chan struct{}
}

// Broker
type Broker struct {
	// lista de nodos a los que transmitir mensajes
	nodes []*node.Node
	// la posición apuntada en `nodes`
	cursor int
	// mapa de respuestas pendientes de la forma `PipitOriginId:work`
	works map[string]*work
	// sincronizador para lectura y escritura
	mu sync.RWMutex

	// canal de recibimiento de nuevos trabajos
	workCh chan *client.Client
	// canal para la devolución de los fragmentos de respuesta de los nodos hacia los clientes
	dispatchCh chan *Message
}

func New() *Broker {
	bk := new(Broker)

	bk.nodes = make([]*node.Node, 0)
	bk.works = make(map[string]*work)
	bk.workCh = make(chan *client.Client)
	bk.dispatchCh = make(chan *Message)

	bk.startWorkListener()
	bk.startDispatchListener()

	return bk
}

// AddNode registra un nodo para recibir trabajo.
func (bk *Broker) AddNode(nd *node.Node) {
	bk.mu.Lock()
	defer bk.mu.Unlock()
	bk.nodes = append(bk.nodes, nd)
}

// ServeHTTP permite usar el Broker directamente como http.Handler.
//
// Este handler es el único que escribe en el ResponseWriter: recibe los
// fragmentos de la respuesta por su propio canal y los va enviando (con flush)
// al cliente. Así no se escribe nunca sobre un ResponseWriter que ya terminó.
func (bk *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := newID()
	r.Header.Set(originHeader, id) // viaja hasta el nodo

	wk := &work{ch: make(chan *Message, 16), gone: r.Context().Done()}

	bk.mu.Lock()
	bk.works[id] = wk
	bk.mu.Unlock()

	defer func() {
		bk.mu.Lock()
		delete(bk.works, id)
		bk.mu.Unlock()
	}()

	bk.workCh <- client.New(w, r)

	rc := http.NewResponseController(w)

	for {
		select {
		case <-r.Context().Done(): // el cliente se desconectó
			return

		case m := <-wk.ch:
			if m.Status != 0 {
				for k, v := range m.Header {
					w.Header()[k] = v
				}
				w.WriteHeader(m.Status)
				rc.Flush() // el cliente recibe los headers de inmediato
			}
			if len(m.Data) > 0 {
				w.Write(m.Data)
				rc.Flush()
			}
			if m.Last {
				return
			}
		}
	}
}

// startWorkListener toma trabajos nuevos y los reparte round-robin.
func (bk *Broker) startWorkListener() {
	go func() {
		for c := range bk.workCh {
			id := c.Request().Header.Get(originHeader)
			go bk.forward(c, id)
		}
	}()
}

// startDispatchListener entrega cada fragmento al cliente que lo espera.
func (bk *Broker) startDispatchListener() {
	go func() {
		for m := range bk.dispatchCh {
			bk.mu.RLock()
			wk := bk.works[m.ID]
			bk.mu.RUnlock()

			if wk == nil { // el cliente ya se había ido
				continue
			}

			select {
			case wk.ch <- m:
			case <-wk.gone:
			}
		}
	}()
}

// next devuelve el siguiente nodo (round-robin) o nil si no hay ninguno.
func (bk *Broker) next() *node.Node {
	bk.mu.Lock()
	defer bk.mu.Unlock()

	for i := 0; i < len(bk.nodes); i++ {
		nd := bk.nodes[bk.cursor]
		bk.cursor = (bk.cursor + 1) % len(bk.nodes)
		if nd.Healthy() {
			return nd
		}
	}
	return nil // ningún nodo sano
}

// forward envía la petición a un nodo y deja la respuesta en dispatchCh.
// Prueba cada nodo hasta que alguno responda; si ninguno lo hace, devuelve un error al cliente.
// Solo reintenta mientras no haya empezado la respuesta: una vez que un nodo responde,
// el resultado (incluido un stream) se reenvía tal cual.
func (bk *Broker) forward(c *client.Client, id string) {
	in := c.Request()

	// El body se lee una sola vez para poder reenviarlo en cada intento.
	body, err := io.ReadAll(in.Body)
	if err != nil {
		bk.fail(id, http.StatusBadRequest, err)
		return
	}

	bk.mu.RLock()
	total := len(bk.nodes)
	bk.mu.RUnlock()

	lastErr := errNoNodes

	for i := 0; i < total; i++ {
		nd := bk.next()
		if nd == nil {
			break
		}

		out, err := http.NewRequestWithContext(in.Context(), in.Method, "http://node"+in.RequestURI, bytes.NewReader(body))
		if err != nil {
			lastErr = err
			break
		}
		out.Header = in.Header.Clone() // incluye Pipit-Origin-Id y Last-Event-ID

		resp, err := nd.Do(out)
		if err != nil {
			// Si el cliente se fue, no tiene sentido seguir probando nodos.
			if in.Context().Err() != nil {
				return
			}
			lastErr = err
			continue // probar con el siguiente nodo
		}

		bk.stream(id, resp)
		return
	}

	status := http.StatusBadGateway
	if errors.Is(lastErr, errNoNodes) {
		status = http.StatusServiceUnavailable
	}
	bk.fail(id, status, lastErr)
}

// stream reenvía la respuesta del nodo a medida que llega, en fragmentos.
func (bk *Broker) stream(id string, resp *http.Response) {
	defer resp.Body.Close()

	h := resp.Header.Clone()
	for _, k := range []string{"Connection", "Keep-Alive", "Transfer-Encoding"} {
		h.Del(k) // cabeceras que solo valen para un salto
	}
	bk.dispatchCh <- &Message{ID: id, Status: resp.StatusCode, Header: h}

	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// buf se reutiliza, así que cada fragmento lleva su propia copia.
			bk.dispatchCh <- &Message{ID: id, Data: append([]byte(nil), buf[:n]...)}
		}
		if err != nil { // io.EOF o el nodo/cliente se cortó
			bk.dispatchCh <- &Message{ID: id, Last: true}
			return
		}
	}
}

// fail responde un error al cliente sin exponer detalles internos.
func (bk *Broker) fail(id string, status int, err error) {
	log.Printf("balancer: petición %s: %v", id, err)

	bk.dispatchCh <- &Message{
		ID:     id,
		Status: status,
		Header: http.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Data:   []byte(http.StatusText(status) + "\n"),
		Last:   true,
	}
}

func newID() string {
	return uuid.New().String()
}
