package com.lta.backend.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lta.backend.services.ProductCatalogService;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ProductQueryListener {
    private final ProductCatalogService catalog;
    private final ObjectMapper json;
    private final KafkaTemplate<String, String> kafka;

    public ProductQueryListener(ProductCatalogService catalog, ObjectMapper json, KafkaTemplate<String, String> kafka) {
        this.catalog = catalog;
        this.json = json;
        this.kafka = kafka;
    }

    @KafkaListener(topics = "product-query-request", groupId = "product-query-workers")
    public void onQuery(ConsumerRecord<String, String> request) throws Exception {
        String response = process(request);
        // Confirmar la publicación antes de terminar el listener: un fallo permite reintento.
        kafka.send("product-query-response", request.key(), response).get(6, TimeUnit.SECONDS);
    }

    public String process(ConsumerRecord<String, String> request) throws JsonProcessingException {
        var products = catalog.search(request.value());
        return json.writeValueAsString(Map.of(
            "requestId", request.key(), "query", request.value(), "products", products,
            "total", products.size(), "processedAt", Instant.now().toString(),
            "requestTopic", request.topic(), "requestPartition", request.partition(),
            "requestOffset", request.offset()));
    }
}
