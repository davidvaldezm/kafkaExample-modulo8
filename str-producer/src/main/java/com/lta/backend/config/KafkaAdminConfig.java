package com.lta.backend.config;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;

@Configuration
public class KafkaAdminConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    // Configura la conexión administrativa con Kafka
    @Bean
    public KafkaAdmin kafkaAdmin(){
        var configs = new HashMap<String,Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    // Crea automáticamente el tópico "str-topic" al iniciar la aplicación
    @Bean
    public KafkaAdmin.NewTopics topics(){
        return new KafkaAdmin.NewTopics(
            TopicBuilder.name("str-topic").partitions(2).replicas(1).build(),
            TopicBuilder.name("str-topic-response").partitions(2).replicas(1).build(),
            TopicBuilder.name("product-query-request").partitions(2).replicas(1).build(),
            TopicBuilder.name("product-query-response").partitions(2).replicas(1).build()
        );
    }
}
