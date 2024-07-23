package com.example.publish.service;

import com.example.publish.model.Notification;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class NotificationProducerService {

    private final ReactiveMessageSender<String> emailProducer;

    private final ReactiveMessageSender<String> smsProducer;

    private final ReactiveMessageSender<String> pushProducer;

    @Autowired
    public NotificationProducerService(ReactiveMessageSender<String> emailProducer, ReactiveMessageSender<String> smsProducer, ReactiveMessageSender<String> pushProducer) {
        this.emailProducer = emailProducer;
        this.smsProducer = smsProducer;
        this.pushProducer = pushProducer;
    }

    public Mono<MessageId> publishNotification(Notification notification) {
        ReactiveMessageSender<String> producer;
        switch (notification.getChannel()) {
            case "email":
                producer = emailProducer;
                break;
            case "sms":
                producer = smsProducer;
                break;
            case "push_notification":
                producer = pushProducer;
                break;
            default:
                return Mono.error(new IllegalArgumentException("Unknown notification channel: " + notification.getChannel()));
        }

        return producer.sendOne(MessageSpec.of(notification.toString()));
    }
}