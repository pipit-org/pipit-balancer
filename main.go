package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/pipit-org/pipit-balancer/broker"
	"github.com/pipit-org/pipit-balancer/node"
)

/*
Las flags permiten introducir argumentos a un programa de consola.

Por ejemplo,

pipit -listen ":8080" -nodes "127.0.0.1:9001,127.0.0.1:9002"

Estos parametros le dice a pipit en que dirección levantar el servidor del balanceador y que nodos tiene que escuchar
*/

var (
	listenFlag string
	nodesFlag  string
)

func main() {
	flag.StringVar(&listenFlag, "listen", ":8080", "dirección donde escucha el balanceador")
	flag.StringVar(&nodesFlag, "nodes", "127.0.0.1:9001,127.0.0.1:9002", "direcciones de los nodos, separadas por coma")
	flag.Parse()

	bk := broker.New()

	for _, plainAddr := range strings.Split(nodesFlag, ",") {
		addr, err := net.ResolveTCPAddr("tcp", strings.TrimSpace(plainAddr))
		if err != nil {
			log.Fatalf("dirección de nodo inválida %q: %v", plainAddr, err)
		}

		bk.AddNode(node.New(addr))
		log.Printf("Nodo registrado: %s", addr)
	}

	log.Printf("Balanceador escuchando en %s", listenFlag)
	if err := http.ListenAndServe(listenFlag, bk); err != nil {
		log.Fatal(err)
	}
}
