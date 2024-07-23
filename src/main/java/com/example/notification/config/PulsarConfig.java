package com.example.notification.config;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConfig {

    @Bean
    public PulsarClient pulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
    }

    @Bean
    public ReactivePulsarClient reactorPulsarClient(PulsarClient pulsarClient) {
        return AdaptedReactivePulsarClientFactory.create(pulsarClient);
    }

    @Bean
    public ReactiveMessageSender<String> emailProducer(ReactivePulsarClient reactorPulsarClient) {
        return reactorPulsarClient
                .messageSender(Schema.STRING)
                .topic("notifications-email")
                .maxInflight(100)
                .build();
    }

    @Bean
    public ReactiveMessageSender<String> smsProducer(ReactivePulsarClient reactorPulsarClient) {
        return reactorPulsarClient
                .messageSender(Schema.STRING)
                .topic("notifications-sms")
                .maxInflight(100)
                .build();
    }

    @Bean
    public ReactiveMessageSender<String> pushProducer(ReactivePulsarClient reactorPulsarClient) {
        return reactorPulsarClient
                .messageSender(Schema.STRING)
                .topic("notifications-push")
                .maxInflight(100)
                .build();
    }
}