package com.lta.backend.resources;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<Map<String, String>> timeout(TimeoutException error) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
            .body(Map.of("error", "Kafka no respondió en 8 segundos. Verifica el broker y str-consumer; vuelve a consultar."));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> invalid(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(Map.of("error", error.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> unavailable(Exception error) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(Map.of("error", "No fue posible enviar la consulta. Verifica Kafka e intenta de nuevo."));
    }
}
