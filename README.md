# Pipit Balancer

Balanceador de carga diseñado para el sistema de mensajería **Pipit**.

## ¿Qué es?

Pipit Balancer actúa como un proxy en un ambiente distribuido. Su función principal es recibir las peticiones de los clientes, distribuirlas entre los nodos disponibles (hoy, de forma equitativa mediante *round-robin*) y retornar las respuestas correspondientes, operando de manera transparente respecto al contenido de los mensajes.

* Al balanceador no le interesa el *qué* ni el *cómo*: solo la distribución de la carga de trabajo.

```
Clientes ──► Pipit Balancer ──► Nodos (Pipit API 1..N)
```

## Componentes principales

* **Broker:** implementa `http.Handler`, por lo que se usa directamente como servidor HTTP. Recibe las peticiones, elige un nodo y devuelve la respuesta al cliente.
* **Node:** representa un nodo del sistema. Es un cliente HTTP que siempre se conecta a la dirección del nodo, y lleva un indicador de salud para que el balanceador pueda omitirlo.
* **Client:** agrupa la petición de un cliente con el `ResponseWriter` donde se escribirá la respuesta.

## Comunicación

* **Identificación de peticiones:** cada petición entrante recibe un encabezado HTTP llamado `Pipit-Origin-Id`, un identificador único que viaja hasta el nodo y que permite asociar la respuesta con el cliente que la espera.
* **Respuestas transparentes:** el código de estado, los encabezados y el cuerpo de la respuesta del nodo llegan al cliente tal como fueron enviados.
* **Streaming:** las respuestas se reenvían a medida que el nodo las produce, en fragmentos de hasta 32 KB y con *flush* inmediato. Esto permite conexiones de larga duración, como los *Server-Sent Events* que usa Pipit para el chat en tiempo real.

## Tolerancia a fallos

* **Reintento en otro nodo:** si la conexión con un nodo falla antes de que empiece la respuesta, el balanceador prueba con el siguiente (como máximo una vez por nodo), sin que el cliente lo note.
* **Errores claros:** si ningún nodo responde, el cliente recibe `502 Bad Gateway`; si no hay nodos disponibles, `503 Service Unavailable`. El detalle del error queda en el log del balanceador y no se expone al cliente.
* **Cortes durante un stream:** una vez que un nodo empezó a responder, no se cambia de nodo. Si el nodo cae en medio de un stream, la conexión se cierra y el cliente puede reconectar (el navegador lo hace solo y envía `Last-Event-ID`, que el balanceador reenvía al nodo).
* **Chequeo de salud (opcional):** `StartHealthCheck` consulta periódicamente un endpoint de cada nodo (por ejemplo, `/health`) y deja de enviarle tráfico mientras no responda `2xx`. Se reincorpora solo cuando se recupera.

## Uso

Requiere **Go 1.27 o superior**.

```go
bk := broker.New()
bk.AddNode(node.New(addr))
http.ListenAndServe(":8080", bk)
```

Los nodos deben registrarse con `AddNode` antes de recibir tráfico.

## Limitaciones

* **Punto único de fallo:** el propio balanceador no está replicado.
* **Un cliente muy lento puede frenar a los demás:** hay un único despachador de respuestas y cada petición tiene un *buffer* de 16 fragmentos; si se llena, el despachador espera a ese cliente (o a que se desconecte).
* **Los reintentos pueden duplicar efectos:** si un nodo procesó una petición y falló antes de responder, el reintento la repite en otro nodo. Para peticiones que no son idempotentes (por ejemplo, publicar un mensaje) esto podría duplicar el resultado.
* **El cuerpo de la petición se lee completo** antes de reenviarse (necesario para poder reintentar), por lo que no sirve para subidas de archivos muy grandes.
* **Solo se descartan algunas cabeceras de salto** (`Connection`, `Keep-Alive` y `Transfer-Encoding`) y no se agrega `X-Forwarded-For`.

## Contexto

Software desarrollado como proyecto de ejemplo para la asignatura **Desarrollo de Aplicaciones para Ambientes Distribuidos** del **IES6017** (Salta, Argentina).

> Este software se distribuye sin garantías de ningún tipo.
