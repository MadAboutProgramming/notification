package com.example.notification.service;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DLQConsumerService {

    private final ReactivePulsarClient reactorPulsarClient;

    @Autowired
    public DLQConsumerService(ReactivePulsarClient reactorPulsarClient) {
        this.reactorPulsarClient = reactorPulsarClient;
    }

    public void init() {
        consumeDLQMessages("notifications-email-dlq", "email-dlq-subscription");
        consumeDLQMessages("notifications-sms-dlq", "sms-dlq-subscription");
        consumeDLQMessages("notifications-push-dlq", "push-dlq-subscription");
    }

    public void consumeDLQMessages(String dlqTopic, String subscription) {
        ReactiveMessageConsumer<String> messageConsumer = reactorPulsarClient.messageConsumer(Schema.STRING)
                .topic(dlqTopic)
                .subscriptionName(subscription)
                .build();

        messageConsumer.consumeMany(messageFlux ->
                messageFlux.flatMap(msg -> {
                    String content = new String(msg.getData());
                    System.out.printf("Received %s message: %s%n", dlqTopic, content);
                    MessageResult.acknowledge(msg);
                    return null;
                })
        ).subscribe();
    }
}