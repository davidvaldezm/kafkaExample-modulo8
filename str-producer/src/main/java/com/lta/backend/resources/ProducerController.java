package com.lta.backend.resources;

import com.lta.backend.services.StringProducerService;
import java.util.concurrent.CompletableFuture;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class ProducerController {
    private final StringProducerService producer;
    public ProducerController(StringProducerService producer) { this.producer = producer; }

    @GetMapping("/menu")
    public CompletableFuture<String> sendMenu(@RequestParam String message) {
        return producer.sendMessage(message);
    }

    @GetMapping("/test")
    public CompletableFuture<String> sendTest(@RequestParam String message) {
        return producer.sendMessage(message);
    }
}
