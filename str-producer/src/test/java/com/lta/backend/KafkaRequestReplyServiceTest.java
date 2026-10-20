package com.lta.backend;

import com.lta.backend.services.KafkaRequestReplyService;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaRequestReplyServiceTest {
    @Test void missingReplyTimesOutAndPublishFailureDoesNotHang() {
        @SuppressWarnings("unchecked") KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), nullable(Integer.class), anyString(), anyString())).thenReturn(new CompletableFuture<>());
        var service = new KafkaRequestReplyService(kafka, 40);
        var error = assertThrows(CompletionException.class, () -> service.request("topic", null, "tofu").join());
        assertInstanceOf(TimeoutException.class, error.getCause());
        when(kafka.send(anyString(), nullable(Integer.class), anyString(), anyString()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Broker unavailable")));
        assertInstanceOf(IllegalStateException.class,
            assertThrows(CompletionException.class, () -> service.request("topic", null, "tofu").join()).getCause());
    }
}
