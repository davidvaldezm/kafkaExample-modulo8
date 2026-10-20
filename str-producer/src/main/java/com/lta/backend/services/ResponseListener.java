package com.lta.backend.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ResponseListener {
    private final KafkaRequestReplyService replies;
    public ResponseListener(KafkaRequestReplyService replies) { this.replies = replies; }

    // Cada instancia recibe las respuestas y resuelve solo sus UUID pendientes.
    @KafkaListener(topics = {"str-topic-response", "product-query-response"},
            groupId = "${app.reply-group}")
    public void listen(ConsumerRecord<String, String> record) {
        replies.complete(record.key(), record.value());
    }
}
