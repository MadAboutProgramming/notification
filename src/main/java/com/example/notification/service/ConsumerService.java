package com.example.notification.service;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    private final ReactivePulsarClient reactorPulsarClient;

    @Autowired
    public ConsumerService(ReactivePulsarClient reactorPulsarClient) {
        this.reactorPulsarClient = reactorPulsarClient;
    }

    public void init() {
        consumeMessages("notifications-email", "email-subscription", "notifications-email-dlq");
        consumeMessages("notifications-sms", "sms-subscription", "notifications-sms-dlq");
        consumeMessages("notifications-push", "push-subscription", "notifications-push-dlq");
    }

    public void consumeMessages(String topic, String subscription, String dlqTopic) {
        ReactiveMessageConsumer<String> messageConsumer = reactorPulsarClient.messageConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscription)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(3)
                        .deadLetterTopic(dlqTopic)
                        .build())
                .build();

        messageConsumer.consumeMany(messageFlux ->
                messageFlux.flatMap(msg -> {
                    String content = new String(msg.getData());
                    System.out.printf("Received %s message: %s%n", topic, content);
                    // Process message
                    // Simulate processing failure for demonstration
                    if (Math.random() > 0.5) {
                        MessageResult.negativeAcknowledge(msg);
                    }
                    MessageResult.acknowledge(msg);
                    return null;
                })
        ).subscribe();
    }
}
