package broker

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"uuid"

	"github.com/pipit-org/pipit-balancer/client"
	"github.com/pipit-org/pipit-balancer/node"
)

const originHeader = "Pipit-Origin-Id"

type Message struct {
	ID   string
	Data []byte
}

// Broker
type Broker struct {
	// lista de nodos a los que transmitir mensajes
	nodes []*node.Node
	// la posición apuntada en `nodes`
	cursor int
	// mapa de clientes de la forma `PipitOriginId:Client`
	works map[string]*client.Client
	// señal de "trabajo terminado" por petición, para que el handler HTTP
	// espere (el http.ResponseWriter deja de ser válido cuando el handler retorna)
	done map[string]chan struct{}
	// sincronizador para lectura y escritura
	mu sync.RWMutex

	// canal de recibimiento de nuevos trabajos
	workCh chan *client.Client
	// canal para la devolución de los trabajos completados por los nodos a los clientes
	dispatchCh chan *Message
}

func New() *Broker {
	bk := new(Broker)

	bk.nodes = make([]*node.Node, 0)
	bk.works = make(map[string]*client.Client)
	bk.done = make(map[string]chan struct{})
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
func (bk *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := newID()
	r.Header.Set(originHeader, id)

	c := client.New(w, r)
	done := make(chan struct{})

	bk.mu.Lock()

	bk.works[id] = c
	bk.done[id] = done

	bk.mu.Unlock()

	bk.workCh <- c

	select {
	case <-done:
	case <-r.Context().Done(): // el cliente se desconectó
		bk.mu.Lock()

		delete(bk.works, id)
		delete(bk.done, id)

		bk.mu.Unlock()
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

// startDispatchListener devuelve cada respuesta al cliente que la espera.
func (bk *Broker) startDispatchListener() {
	go func() {
		for m := range bk.dispatchCh {
			bk.mu.Lock()

			c := bk.works[m.ID]
			done := bk.done[m.ID]

			delete(bk.works, m.ID)
			delete(bk.done, m.ID)

			bk.mu.Unlock()

			if c == nil { // el cliente ya se había ido
				continue
			}

			c.Writer().Write(m.Data)
			close(done)
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

// forward envía la petición al nodo y deja la respuesta en dispatchCh.
// Prueba cada nodo hasta que alguno responda; si ningún nodo está activo, devuelve un error al dispatchCh.
func (bk *Broker) forward(c *client.Client, id string) {
	in := c.Request()

	// El body se lee una sola vez para poder reenviarlo en cada intento.
	body, err := io.ReadAll(in.Body)
	if err != nil {
		bk.dispatchCh <- &Message{ID: id, Data: []byte(err.Error())}
		return
	}

	bk.mu.RLock()
	total := len(bk.nodes)
	bk.mu.RUnlock()

	lastErr := errors.New("sin nodos disponibles")

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
		out.Header = in.Header.Clone() // incluye Pipit-Origin-Id

		resp, err := nd.Do(out)
		if err != nil {
			// Si el cliente se fue, no tiene sentido seguir probando nodos.
			if in.Context().Err() != nil {
				return
			}
			lastErr = err
			continue // probar con el siguiente nodo
		}

		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bk.dispatchCh <- &Message{ID: id, Data: data}
		return
	}

	bk.dispatchCh <- &Message{ID: id, Data: []byte(lastErr.Error())}
}

func newID() string {
	return uuid.New().String()
}
