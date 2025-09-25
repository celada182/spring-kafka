package com.celada.spring.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Objects;


@SpringBootApplication
@EnableScheduling
public class Application implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

//    @KafkaListener(topics = "first-topic", containerFactory = "kafkaListenerContainerFactory", groupId = "first-group", properties = {"max.poll.interval.ms:4000",
//            "max.poll.records:10"})
//    public void listen(List<String> messages) {
//        log.info("Start reading batch");
//        for (String message : messages) {
//            log.info("Message received {}", message);
//        }
//        log.info("Finish reading batch");
//    }

    @KafkaListener(
            id = "first-listener",
            autoStartup = "false",
            topics = "first-topic",
            containerFactory = "kafkaListenerContainerFactory",
            groupId = "first-group",
            properties = {
                    "max.poll.interval.ms:4000",
                    "max.poll.records:10"
            })
    public void listen(List<ConsumerRecord<String, String>> messages) {
        log.info("Start reading batch");
        for (ConsumerRecord<String, String> message : messages) {
            log.info("Partition = {}, Offset = {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(), message.value());
        }
        log.info("Finish reading batch");
    }

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 100; i++) {
            template.send("first-topic", String.valueOf(i), String.format("Sample message %d", i));
        }
        log.info("Waiting to start listener");
        Thread.sleep(5000);
        log.info("Listening");
        Objects.requireNonNull(registry.getListenerContainer("first-listener")).start();
        Thread.sleep(5000);
        log.info("Stop listener");
        Objects.requireNonNull(registry.getListenerContainer("first-listener")).stop();

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

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    public void print() {
        log.info("Scheduled message");
    }
}
