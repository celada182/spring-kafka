package com.celada.spring.kafka;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;


@SpringBootApplication
@EnableScheduling
public class Application {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private MeterRegistry meterRegistry;

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
            autoStartup = "true",
            topics = "first-topic",
            containerFactory = "kafkaListenerContainerFactory",
            groupId = "first-group",
            properties = {
                    "max.poll.interval.ms:4000",
                    "max.poll.records:50"
            })
    public void listen(List<ConsumerRecord<String, String>> messages) {
        log.info("Start reading batch");
        for (ConsumerRecord<String, String> message : messages) {
            log.info("Partition = {}, Offset = {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(), message.value());
        }
        log.info("Finish reading batch");
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 100)
    public void sendMessages() {
//        log.info("Waiting to start listener");
//        Thread.sleep(5000);
//        log.info("Listening");
//        Objects.requireNonNull(registry.getListenerContainer("first-listener")).start();
//        Thread.sleep(5000);
//        log.info("Stop listener");
//        Objects.requireNonNull(registry.getListenerContainer("first-listener")).stop();

        for (int i = 0; i < 200; i++) {
            template.send("first-topic", String.valueOf(i), String.format("Sample message %d", i));
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

    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void messageCount() {
        double count = meterRegistry.get("kafka.producer.record.send.total")
                .functionCounter()
                .count();
        log.info("Total Messages {} ", count);

//        List<Meter> metrics = meterRegistry.getMeters();

//        kafka.producer.outgoing.byte.total
//        kafka.producer.iotime.total
//        kafka.producer.buffer.exhausted.total
//        kafka.producer.io.ratio
//        kafka.producer.node.request.size.avg
//        kafka.producer.record.queue.time.max
//        kafka.producer.successful.authentication.rate
//        kafka.producer.connection.close.rate
//        kafka.producer.outgoing.byte.rate
//        kafka.producer.record.retry.rate
//        kafka.producer.failed.authentication.total
//        kafka.producer.node.request.latency.avg
//        kafka.producer.requests.in.flight
//        kafka.producer.connection.creation.rate
//        kafka.producer.network.io.rate
//        kafka.producer.record.send.total
//        kafka.producer.produce.throttle.time.max
//        kafka.producer.metadata.age
//        kafka.producer.io.wait.ratio
//        kafka.producer.incoming.byte.rate
//        kafka.producer.successful.reauthentication.rate
//        kafka.producer.records.per.request.avg
//        kafka.producer.request.size.avg
//        kafka.producer.node.incoming.byte.total
//        kafka.producer.buffer.available.bytes
//        kafka.producer.select.rate
//        kafka.producer.record.retry.total
//        kafka.producer.reauthentication.latency.max
//        kafka.producer.produce.throttle.time.avg
//        kafka.producer.io.waittime.total
//        kafka.producer.batch.size.avg
//        kafka.producer.node.response.total
//        kafka.producer.request.latency.max
//        kafka.producer.request.size.max
//        spring.kafka.template
//        kafka.producer.connection.close.total
//        kafka.producer.buffer.total.bytes
//        kafka.producer.node.request.size.max
//        kafka.producer.request.rate
//        kafka.producer.record.error.rate
//        kafka.producer.connection.count
//        kafka.producer.network.io.total
//        kafka.producer.node.response.rate
//        kafka.producer.record.send.rate
    }
}
