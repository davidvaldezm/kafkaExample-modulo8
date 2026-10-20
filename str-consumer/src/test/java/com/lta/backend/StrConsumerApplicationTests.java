package com.lta.backend;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = StrConsumerApplication.class)
@org.springframework.kafka.test.context.EmbeddedKafka(partitions = 2,
 topics = {"str-topic", "str-topic-response", "product-query-request", "product-query-response"},
 bootstrapServersProperty = "spring.kafka.bootstrap-servers", kraft = true)
@org.springframework.test.annotation.DirtiesContext
class StrConsumerApplicationTests {

	@Test
	void contextLoads() {
	}

}
