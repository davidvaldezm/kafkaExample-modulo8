package com.lta.backend.services;

import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Service;

@Service
public class StringProducerService {
    private final KafkaRequestReplyService replies;
    public StringProducerService(KafkaRequestReplyService replies) { this.replies = replies; }

    public CompletableFuture<String> sendMessage(String message) {
        int partition = message.contains("menu") ? 1 : 0;
        return replies.request("str-topic", partition, message);
    }
}
