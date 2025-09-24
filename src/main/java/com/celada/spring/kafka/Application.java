package com.celada.spring.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;


@SpringBootApplication
public class Application implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @KafkaListener(topics = "first-topic", containerFactory = "kafkaListenerContainerFactory", groupId = "first-group", properties = {"max.poll.interval.ms:4000",
            "max.poll.records:10"})
    public void listen(List<String> messages) {
        log.info("Start reading batch");
        for (String message : messages) {
            log.info("Message received {}", message);
        }
        log.info("Finish reading batch");
    }

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("first-topic", String.format("Sample message %d", i));
        }
//        String message = "Sample message";
//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("first-topic", message);
//        future.whenCompleteAsync((result, ex) -> {
//            if (ex == null) {
//                log.info("Message sent {} with offset {}", message, result.getRecordMetadata().offset());
//            } else {
//                log.error("Error sending message {}", message);
//            }
//        });
    }
}
