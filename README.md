# Pipit Balancer

Balanceador de carga para el sistema de mensajería Pipit.

## Balanceadores

Un balanceador actúa como un proxy entre los *clientes* y los *nodos* en un ambiente distribuido.

Esencialmente, el balanceador tiene el trabajo de recibir las peticiones de los clientes y distribuirlas bajo algún criterio (por ejemplo, rendimiento o equitatividad) entre los servicios que lo componen, escuchar las respuestas de los servicios y devolver los resultados a los clientes.

A estos sistemas no les interesa ni el *qué* o el *cómo*, sólo la distribución de carga de trabajo.

## Comunicación

Los nodos del sistema pueden devolver cualquier tipo de respuesta como un stream de bytes y el balancer se encargará de hacer llegar esos datos al cliente. Sin embargo, el balanceador necesita alguna forma de identificar y asociar las peticiones con su lista de clientes. Cada petición entrante al balanceador obtendrá un *HTTP Header* llamado `Pipit-Origin-Id` que identifique de manera inequivoca a la petición original. Este identificador tendrá asociado el `Cliente` que hizo la petición y que está esperando una respuesta.

## Relevante

El presente software se distribuye sin garantías de ningún tipo. La intención original de este programa es servir como proyecto de ejemplo para la asignatura "Desarrollo de Aplicaciones para Ambientes Distribuidos" en el IES6017, Salta, Argentina.

