package node

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
)

// Node
type Node struct {
	// Los nodos son servidores HTTP, así que podemos embeberlos como clientes a los cuales hacer peticiones
	*http.Client

	addr    net.Addr
	healthy atomic.Bool
}

// New crea un nuevo Nodo apuntando a la addr proporcionada.
func New(addr net.Addr) *Node {
	nd := new(Node)

	nd.addr = addr
	nd.healthy.Store(true)

	nd.Client = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, addr.Network(), addr.String())
			},
		},
	}

	return nd
}

func (nd *Node) Addr() net.Addr {
	return nd.addr
}

func (nd *Node) Healthy() bool     { return nd.healthy.Load() }
func (nd *Node) SetHealthy(b bool) { nd.healthy.Store(b) }
