# Pipit Balancer

Balanceador de carga diseñado para el sistema de mensajería **Pipit**.

## ¿Qué es?

Pipit Balancer actúa como un proxy en un ambiente distribuido. Su función principal es recibir las peticiones de los clientes, distribuirlas entre los nodos disponibles (según criterios de rendimiento o equitatividad) y retornar las respuestas correspondientes, operando de manera transparente respecto al contenido de los mensajes.

## Comunicación

* **Flujo de datos:** Los nodos pueden enviar cualquier tipo de respuesta, las cuales el balanceador se encargará de reenviar a los clientes.
* **Identificación de clientes:** Para asociar cada petición con su respectivo destino, el balanceador asigna un encabezado HTTP llamado `Pipit-Origin-Id`. Este identificador único permite vincular la solicitud con el cliente que espera la respuesta.

## Contexto

Software desarrollado como proyecto de ejemplo para la asignatura **Desarrollo de Aplicaciones para Ambientes Distribuidos** del **IES6017** (Salta, Argentina).

> Este software se distribuye sin garantías de ningún tipo.
