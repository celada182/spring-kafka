package com.celada.spring.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;


@SpringBootApplication
public class Application implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @KafkaListener(topics = "first-topic", groupId = "first-group")
    public void listen(String message) {
        log.info("Message received {}", message);
    }

    @Override
    public void run(String... args) throws Exception {
        kafkaTemplate.send("first-topic", "Sample message");
    }
}
