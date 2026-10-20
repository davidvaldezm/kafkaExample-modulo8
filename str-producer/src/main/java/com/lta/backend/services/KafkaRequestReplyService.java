package com.lta.backend.services;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** La clave Kafka correlaciona cada solicitud con su propia respuesta. */
@Service
public class KafkaRequestReplyService {
    private final KafkaTemplate<String, String> kafka;
    private final ConcurrentHashMap<String, CompletableFuture<String>> pending = new ConcurrentHashMap<>();
    private final Semaphore capacity = new Semaphore(128);
    private final Duration timeout;

    public KafkaRequestReplyService(KafkaTemplate<String, String> kafka,
            @Value("${app.reply-timeout-ms:8000}") long timeoutMs) {
        this.kafka = kafka;
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    public CompletableFuture<String> request(String topic, Integer partition, String message) {
        if (!capacity.tryAcquire()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Demasiadas consultas pendientes"));
        }
        String requestId = UUID.randomUUID().toString();
        var reply = new CompletableFuture<String>();
        pending.put(requestId, reply);
        reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((value, error) -> {
                pending.remove(requestId, reply);
                capacity.release();
            });
        try {
            kafka.send(topic, partition, requestId, message).whenComplete((sent, error) -> {
                if (error != null) reply.completeExceptionally(error);
            });
        } catch (Exception error) {
            reply.completeExceptionally(error);
        }
        return reply;
    }

    public void complete(String requestId, String message) {
        if (requestId == null) return; // Mensajes antiguos sin correlación.
        var reply = pending.get(requestId);
        if (reply != null) reply.complete(message);
    }
}
