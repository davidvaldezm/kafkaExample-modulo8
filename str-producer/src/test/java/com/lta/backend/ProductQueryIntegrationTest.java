package com.lta.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lta.backend.config.StringConsumerConfig;
import com.lta.backend.listeners.ProductQueryListener;
import com.lta.backend.listeners.StrConsumerListener;
import com.lta.backend.services.OrderProcessor;
import com.lta.backend.services.ProductCatalogService;
import java.net.URI;
import java.net.http.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = StrProducerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ProductQueryListener.class, ProductCatalogService.class, StrConsumerListener.class,
    OrderProcessor.class, StringConsumerConfig.class})
@EmbeddedKafka(partitions = 2, topics = {"str-topic", "str-topic-response", "product-query-request", "product-query-response"},
    bootstrapServersProperty = "spring.kafka.bootstrap-servers", kraft = true)
@org.springframework.test.annotation.DirtiesContext
class ProductQueryIntegrationTest {
    @LocalServerPort int port;
    @Autowired ObjectMapper json;
    private final HttpClient client = HttpClient.newHttpClient();

    private CompletableFuture<HttpResponse<String>> query(String term) {
        return client.sendAsync(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/products?query=" + term))
            .timeout(java.time.Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test void queryCrossesKafkaAndReturnsRealPartitionMetadata() throws Exception {
        var reply = query("tofu").get(20, TimeUnit.SECONDS);
        assertEquals(200, reply.statusCode());
        var data = json.readTree(reply.body());
        assertEquals("PRD-001", data.path("products").get(0).path("id").asText());
        assertEquals("product-query-request", data.path("requestTopic").asText());
        assertTrue(data.path("requestPartition").asInt() < 2);
        assertEquals(1, data.path("total").asInt());
    }

    @Test void concurrentQueriesNeverExchangeResponses() throws Exception {
        var a = query("tofu"); var b = query("pizza");
        var first = json.readTree(a.get(20, TimeUnit.SECONDS).body());
        var second = json.readTree(b.get(20, TimeUnit.SECONDS).body());
        assertEquals("Bowl de tofu", first.path("products").get(0).path("name").asText());
        assertEquals("Pizza margarita", second.path("products").get(0).path("name").asText());
        assertNotEquals(first.path("requestId").asText(), second.path("requestId").asText());
    }

    @Test void emptyNoMatchAndInvalidQueriesHaveExplicitResults() throws Exception {
        assertEquals(6, json.readTree(query("").get(20, TimeUnit.SECONDS).body()).path("total").asInt());
        assertEquals(0, json.readTree(query("zzzzz").get(20, TimeUnit.SECONDS).body()).path("total").asInt());
        assertEquals(400, query("a".repeat(81)).get(20, TimeUnit.SECONDS).statusCode());
    }

    @Test void originalMenuStillTravelsThroughPartitionOne() throws Exception {
        var response = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/producer/menu?message=veganmenu"))
            .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("Vegan menu"));
    }

    @Test void requestWithoutResponderReturnsGatewayTimeout() throws Exception {
        var response = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/producer/test?message=solo-log"))
            .timeout(java.time.Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(504, response.statusCode());
        assertTrue(response.body().contains("Kafka no respondió"));
    }
}
