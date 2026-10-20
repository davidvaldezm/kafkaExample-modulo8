# Kafka en acción · Módulo 8

Consulta de productos en React, nuevo listener Spring Boot y diagrama de arquitectura integrado. El flujo completo es React → HTTP → str-producer → Kafka → str-consumer → Kafka → str-producer → HTTP → React.

## Repositorios de la entrega

- Front-end: https://github.com/davidvaldezm/frontProducerKafka-modulo8/tree/actividad-modulo8
- Back-end: https://github.com/davidvaldezm/kafkaExample-modulo8/tree/actividad-modulo8

## Requisitos

Java 17, Node.js 22.12 o superior, Docker Desktop activo con contenedores Linux. Maven se descarga mediante el wrapper incluido; no requiere instalación global. Puertos libres: 9092 (Kafka), 19000 (Kafdrop), 8000 (gateway), 8100 (consumer), 5173 (Vite).

## Ejecutar desde cero en Windows / PowerShell

Clona ambos repositorios en carpetas hermanas y selecciona la rama `actividad-modulo8` si tu clon no la trae como predeterminada. Desde la raíz del back-end:

```powershell
docker compose up -d
docker compose ps
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092
.\str-producer\mvnw.cmd -f pom.xml verify
```

Espera a que el comando del broker responda antes de seguir. La compilación ejecuta 9 pruebas con Kafka embebido independiente del broker Docker. En una terminal, inicia primero el gateway:

```powershell
java -jar str-producer/target/str-producer-0.0.1-SNAPSHOT-exec.jar
```

Espera `Started StrProducerApplication`. El gateway crea los cuatro topics con 2 particiones y factor de replicación 1. En otra terminal inicia el procesador:

```powershell
java -jar str-consumer/target/str-consumer-0.0.1-SNAPSHOT-exec.jar
```

Espera a que `product-query-workers` tenga asignadas P0 y P1. En el front-end:

```powershell
npm ci
npm run dev -- --host 127.0.0.1
```

Abre http://127.0.0.1:5173. Deja Producto vacío y pulsa **Consultar por Kafka** para ver los seis productos, o busca `tofu`, `vegano`, `BÓWL` o `PRD-004`. La pestaña **Arquitectura** contiene la imagen, ampliación y descargas. **Menús originales** conserva el ejercicio base (`vegan`, `vegetarian`, `meat`).

En Linux/macOS: `./str-producer/mvnw -f pom.xml verify` (si es necesario, `chmod +x str-producer/mvnw`). Los comandos `java -jar` y `npm` son iguales. Si Java no se detecta, configura `JAVA_HOME` al JDK 17.

## Comprobar el recorrido

```powershell
Invoke-RestMethod 'http://localhost:8000/api/products?query=tofu'
Invoke-RestMethod 'http://localhost:8000/producer/menu?message=veganmenu'
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic 'str-topic|str-topic-response|product-query-request|product-query-response'
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group product-query-workers
```

Kafdrop está en http://localhost:19000. Puedes inspeccionar solicitudes y respuestas que comparten la clave UUID. La pantalla muestra el UUID, partición y offset reales de la solicitud. Se conserva HTTP abierto hasta recibir su respuesta Kafka; no se usa WebSocket.

## Pruebas y evidencia

Desde back-end: `str-producer/mvnw.cmd -f pom.xml verify` (9 pruebas, incluyendo 5 casos de integración HTTP + Kafka embebido, correlación concurrente y timeout 504). Desde front-end:

```powershell
npm run lint
npm run build
npx playwright install msedge
npm run test:e2e
```

Las pruebas usan Edge y requieren ambos back-ends y Vite ejecutándose. Si Edge ya está instalado no necesitas instalarlo. Para otro puerto: `$env:E2E_BASE_URL='http://127.0.0.1:5174'`. Las seis pruebas verifican el catálogo real, búsqueda con acentos/sin resultados, pestañas por teclado, imagen y ampliación, móvil, menú original y estado de error. Solo el último caso inyecta un HTTP 503 para comprobar la interfaz; los casos de éxito usan Kafka real.

La evidencia se encuentra en `docs/evidence/`: capturas de escritorio y móvil, reporte Playwright, respuesta HTTP, particiones/réplicas y resumen de pruebas. En esta máquina se usó temporalmente Vite :5174 durante la verificación para aislar la sesión del navegador.

## Entregables y explicación

- `docs/componentes.md`: tabla completa de identificación, clases, métodos y explicación de decisiones (back-end).
- `docs/identificacion-componentes.pdf`: documento breve (ambos repositorios).
- `docs/architecture.svg` y `docs/architecture.png` (back-end); `public/architecture.svg` y `.png` (front-end): mismo diagrama integrado.
- `docs/evidence/README.md`: resultados comprobados y capturas.

Catálogo de demostración en `str-consumer/src/main/java/com/lta/backend/services/ProductCatalogService.java`: seis productos de menús, precios MXN y existencias en memoria. React no incorpora productos ni respuestas simuladas.

## Configuración y decisiones

`KAFKA_BOOTSTRAP_SERVERS` permite cambiar el broker (default localhost:9092). `VITE_API_BASE_URL` permite configurar un back-end en un despliegue estático; por defecto Vite y su preview redirigen `/api` y `/producer` a localhost:8000. El CORS directo del gateway admite localhost:5173; para otro origen usa el proxy o configura `CorsConfig`.

Los cuatro topics tienen `.partitions(2).replicas(1)` en `KafkaAdminConfig.topics()`. La replicación 1 incluye la copia líder; no hay redundancia frente a caída del broker. `test-topic` es un ejemplo del README original y no participa en la aplicación. Los consumers no crean topics automáticamente: los administra el gateway.

Se sustituyó la variable estática de respuesta por un mapa de futures correlacionados con UUID. El grupo de respuestas es único por instancia del gateway; el grupo del nuevo listener es `product-query-workers`. Hay hasta 128 peticiones pendientes y 8 segundos de espera; HTTP 400 indica búsqueda inválida (>80 caracteres), 504 falta de respuesta y 503 error de publicación. El endpoint original sin `menu` usa P0 y recibe 504 porque `listener1` solo registra.

Si obtienes 504, comprueba el broker y que `str-consumer` esté activo y tenga las dos particiones asignadas. Si una instalación antigua ya creó topics con otra cantidad de particiones/réplicas, inspecciónalos antes: esta entrega no borra mensajes ni reduce particiones. Corrige la topología o usa un entorno Docker limpio según tu necesidad.

## Verificación del original

Se conservaron las revisiones originales front `7384e0c` y back `f57f607` como referencia. `npm install` seguido de `npm run build` falló porque el front importaba `react-router-dom` sin declararlo. Ambos back-ends originales se compilaron, arrancaron y respondieron `Vegan menu: tofu bowl, quinoa salad, green smoothie` a la petición original. La evidencia `baseline-menu.txt` conserva esa respuesta. La entrega agrega la dependencia faltante y comprueba de nuevo el menú en el navegador.

Para detener: Ctrl+C en las terminales Java y Vite; después `docker compose down`. No es necesario borrar volúmenes ni ejecutar limpiezas globales.
