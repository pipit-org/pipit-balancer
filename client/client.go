package client

import (
	"net/http"
)

// Node
type Client struct {
	// Interfaz para comunicarse con el cliente
	rw http.ResponseWriter
	// Petición del cliente
	req *http.Request
}

func New(w http.ResponseWriter, r *http.Request) *Client {
	c := new(Client)
	c.rw = w
	c.req = r

	return c
}

func (c *Client) Request() *http.Request {
	return c.req
}

func (c *Client) Writer() http.ResponseWriter {
	return c.rw
}
