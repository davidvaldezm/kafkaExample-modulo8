package com.lta.backend.resources;

import com.lta.backend.services.KafkaRequestReplyService;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/products")
public class ProductQueryController {
    private final KafkaRequestReplyService replies;
    public ProductQueryController(KafkaRequestReplyService replies) { this.replies = replies; }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<String> query(@RequestParam(defaultValue = "") String query) {
        if (query.length() > 80) throw new IllegalArgumentException("La búsqueda admite hasta 80 caracteres");
        return replies.request("product-query-request", null, query.strip());
    }
}
