package com.haykz;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class KafkaService {
    private KafkaConsumer<String, String> consumer;
    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    public KafkaService(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-service-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
        } catch (Exception e) {
            logger.error("An error occurred: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create Kafka consumer", e);
        }
    }

    public void consumeMessages(Consumer<String> messageProcessor) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received message: {}", record.value());
                messageProcessor.accept(record.value());
            }
        }
    }

}
