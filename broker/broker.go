package broker

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
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
	//bk.startHealthCheck()

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

			nd := bk.next()
			if nd == nil {
				bk.dispatchCh <- &Message{ID: id, Data: []byte("sin nodos disponibles")}
				continue
			}

			go bk.forward(nd, c, id)
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

func (bk *Broker) startHealthCheck() {
	// TODO
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
func (bk *Broker) forward(nd *node.Node, c *client.Client, id string) {
	in := c.Request()

	// El host es irrelevante: el Transport del nodo siempre marca a su addr.
	out, err := http.NewRequestWithContext(in.Context(), in.Method, "http://node"+in.RequestURI, in.Body)
	if err != nil {
		bk.dispatchCh <- &Message{ID: id, Data: []byte(err.Error())}
		return
	}
	out.Header = in.Header.Clone() // incluye Pipit-Origin-Id
	out.ContentLength = in.ContentLength

	resp, err := nd.Do(out)
	if err != nil {
		bk.dispatchCh <- &Message{ID: id, Data: []byte(err.Error())}
		return
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		data = []byte(err.Error())
	}
	bk.dispatchCh <- &Message{ID: id, Data: data}
}

func (bk *Broker) check(ctx context.Context, nd *node.Node, path string, timeout time.Duration) {
	// TODO
}

func newID() string {
	return uuid.New().String()
}
