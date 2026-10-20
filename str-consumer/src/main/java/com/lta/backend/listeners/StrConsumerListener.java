package com.lta.backend.listeners;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.lta.backend.services.OrderProcessor;

@Log4j2
@Component
public class StrConsumerListener {
    
    @Autowired
    private OrderProcessor orderProcessor;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // Listener que pertenece al grupo "group-1"
    // Solo consume mensajes de la partición 0 del tópico "str-topic"
    @KafkaListener(groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "str-topic",partitions = {"0"})
            ,containerFactory = "validMessageContainerFactory")
    public void listener1(String message){
        log.info("LISTENER1 ::: Recibiendo un mensaje {}",message);
    }

    @KafkaListener(groupId = "group-1",
        topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}),
        containerFactory = "validMessageContainerFactory")
    public void listener2(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) throws Exception {
        String message = record.value();

        log.info("Message received: {}", message);

        String menu = orderProcessor.process(message);

        log.info("Suggested menu: {}", menu);

        //  RESPUESTA
        kafkaTemplate.send("str-topic-response", record.key(), menu).get(6, java.util.concurrent.TimeUnit.SECONDS);
    }

    // Listener en un grupo diferente: "group-2"
    // Consume TODOS los mensajes del tópico "str-topic" (todas las particiones)
    @KafkaListener(groupId = "group-2",topics = "str-topic",containerFactory = "validMessageContainerFactory")
    public void listener3(String message){
        log.info("LISTENER3 ::: Recibiendo un mensaje {}",message);
    }

}






