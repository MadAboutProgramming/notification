package com.example.notification.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Notification {
    private int id;
    private String channel;
    private String recipient;
    private String message;
    private String timestamp;

    @Override
    public String toString() {
        return String.format("{\"id\":%d,\"channel\":\"%s\",\"recipient\":\"%s\",\"message\":\"%s\",\"timestamp\":\"%s\"}", id, channel, recipient, message, timestamp);
    }
}