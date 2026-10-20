# Identificación de componentes · Módulo 8

Las rutas son relativas a `kafkaExample`. El paquete Java base es `com.lta.backend`. La tabla distingue el código original de la ampliación. Los topics de aplicación se crean mediante `KafkaAdminConfig.topics()`, no mediante los ejemplos de consola del README original.

| Componente | Archivo y clase | Método o configuración | Papel en el flujo |
|---|---|---|---|
| Producer original | `str-producer/src/main/java/com/lta/backend/services/StringProducerService.java` · `StringProducerService` | `sendMessage(message)`; antes enviaba directamente con `KafkaTemplate.send`; ahora delega en `KafkaRequestReplyService.request()` | Publica en `str-topic`. Si contiene `menu`, elige P1; si no, P0. La clave UUID se agregó para correlación. |
| Producer de respuesta | `str-consumer/src/main/java/com/lta/backend/listeners/StrConsumerListener.java` · `StrConsumerListener` | `listener2(record)` → `OrderProcessor.process()` → `kafkaTemplate.send()` | Consume de P1 y publica el menú en `str-topic-response`, conservando la clave de la solicitud. Un componente puede ser consumer y producer. |
| Consumer | `str-consumer/src/main/java/com/lta/backend/listeners/StrConsumerListener.java` · `StrConsumerListener` | `listener1()` y `listener2()` usan `group-1` y asignación fija P0/P1. `listener3()` usa `group-2`, suscrito a todo `str-topic`. | `listener1` solo registra; `listener2` procesa y responde; `listener3` observa una lectura independiente del grupo 1. No genera tres respuestas. |
| Topics | `str-producer/src/main/java/com/lta/backend/config/KafkaAdminConfig.java` · `KafkaAdminConfig` | `kafkaAdmin()` conecta; `topics()` declara `str-topic`, `str-topic-response` y ahora `product-query-request`, `product-query-response`. | Solicitudes y respuestas son logs separados. `test-topic` es un ejemplo manual, no es usado por React. `__consumer_offsets` es interno. |
| Particiones | `KafkaAdminConfig.java` · `topics()`; `StringProducerService.java` · `sendMessage()`; `StrConsumerListener.java` | Los cuatro `TopicBuilder` usan `.partitions(2)`. El flujo original fija P0/P1 mediante `@TopicPartition`. Productos usa clave UUID y particionador Kafka. | P0 y P1 son logs diferentes; el orden se conserva dentro de una partición, no globalmente. En productos el grupo distribuye las particiones. |
| Réplicas | `KafkaAdminConfig.java` · `topics()` y `docker-compose.yml` · servicio `kafka` | Cada topic declara `.replicas(1)`. El broker configura `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` para el topic interno. | Un broker, una copia por partición; no hay tolerancia a caída del broker. El factor 1 incluye al líder, no significa una copia adicional. |
| Listeners | `StrConsumerListener.java` · `listener1/2/3`; `str-producer/src/main/java/com/lta/backend/services/ResponseListener.java` · `listen(record)` | `@KafkaListener` crea consumidores. `StringConsumerConfig.consumerFactory()` y `validMessageContainerFactory()` configuran los del flujo original. | `ResponseListener` consume respuestas y completa la petición HTTP pendiente. Su grupo original era `group-1`; ahora es único por instancia: `product-replies-${random.uuid}`. |

## Ampliación: consulta de productos

| Archivo (dentro de src/main/java/com/lta/backend) | Clase y función | Responsabilidad |
|---|---|---|
| `str-producer/.../resources/ProductQueryController.java` | `ProductQueryController.query(query)` | `GET /api/products?query=...`; valida longitud <=80; retorna un `CompletableFuture<String>` de JSON. No consulta el catálogo directamente. |
| `str-producer/.../services/KafkaRequestReplyService.java` | `request(topic, partition, message)` y `complete(id, value)` | Genera UUID como clave Kafka; registra un future por solicitud; publica; resuelve su respuesta. Límite 128 pendientes, timeout 8 s y limpieza al terminar. |
| `str-consumer/.../listeners/ProductQueryListener.java` | `onQuery(record)` y `process(record)` | Nuevo `@KafkaListener(topics="product-query-request", groupId="product-query-workers")`; consulta servicio; serializa resultado; publica `product-query-response` con la misma clave. Espera confirmación del envío antes de terminar. |
| `str-consumer/.../services/ProductCatalogService.java` | `search(query)` y `normalize(text)` | Catálogo de 6 productos de demostración en memoria. Filtra por ID, nombre y categoría; ignora mayúsculas y acentos; vacío retorna todos. |
| `str-producer/.../services/ResponseListener.java` | `listen(record)` | Suscrito a ambos topics de respuesta. Usa la clave para resolver solo la solicitud correspondiente. Cada instancia tiene su grupo para recibir sus propias respuestas incluso con varios gateways. |
| `str-producer/.../resources/ApiExceptionHandler.java` | `invalid()`, `timeout()`, `unavailable()` | HTTP 400 por consulta inválida, 504 si no llega respuesta y 503 ante fallo de publicación. |

## Recorrido explicado

1. `App` muestra `Navbar` y `Home`. `Home` conserva la pestaña seleccionada. `ProductQuery.search()` usa `getResponse()` de `src/api.js` para llamar por HTTP al gateway.
2. Vite reenvía `/api` al puerto 8000. `ProductQueryController.query()` valida la búsqueda y llama `KafkaRequestReplyService.request()`.
3. El servicio registra un future con UUID y publica la búsqueda en `product-query-request`; el UUID es la clave Kafka, no un estado global compartido.
4. `ProductQueryListener.onQuery()` consume como parte de `product-query-workers`, ejecuta `ProductCatalogService.search()` y publica el JSON en `product-query-response` con la misma clave.
5. `ResponseListener.listen()` recibe el resultado y llama `complete(UUID, valor)`. Spring completa la respuesta HTTP pendiente y React dibuja la tabla. El JSON muestra el topic, partición y offset de la solicitud como traza real.
6. `ArchitectureView` muestra la imagen SVG, permite ampliar y descargar SVG/PNG. La pestaña no genera tráfico Kafka.

No hay WebSocket. Kafka está entre las dos aplicaciones Spring Boot, no entre el navegador y el broker. HTTP es asíncrono en el servidor con `CompletableFuture`; el navegador espera una respuesta normal.

## Lectura crítica del original

- La variable `ResponseListener.response` era estática y compartida; solicitudes concurrentes podían recibir respuestas cruzadas. Se reemplazó por correlación con UUID.
- `StringConsumerConfig.validMessage()` es un interceptor que **devuelve siempre el record**. Su log dice "ignorado", pero no filtra mensajes. Para descartar tendría que devolver `null`.
- `str-consumer/services/clientes.java` no se invoca en los flujos observados.
- `KAFKA_LISTENERS` y `KAFKA_ADVERTISED_LISTENERS` configuran conexiones de red del broker; no son los listeners de aplicación anotados con `@KafkaListener`.
- Se desactivó la creación automática de topics en broker y consumers. El gateway administra los cuatro topics con dos particiones antes de iniciar el consumer. La metadata de consumers se actualiza cada 5 s.
- La entrega es una demostración local: catálogo en memoria, sin autenticación ni base de datos. Kafka ofrece entrega con posibles reintentos; no se afirma procesamiento exactamente una vez. Las consultas son de solo lectura y una respuesta duplicada no resuelve dos veces un future.

## Fuentes

- Código original front: https://github.com/franciscogomez2722/frontProducerKafka/tree/7384e0cb96c6367e6b93b7a31d8298ccfcb41d55
- Código original back: https://github.com/franciscogomez2722/kafkaExample/tree/f57f6074973d543375e7a2bf6a1090c41fb5b5f5
- Apache Kafka 3.5, diseño: https://kafka.apache.org/35/design/design/
- Spring Kafka, publicación y request/reply: https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html

La identificación deriva del código suministrado. Las diapositivas del Módulo 8 no fueron adjuntadas; no se les atribuyen citas o contenido no consultado.
