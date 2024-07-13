package com.haykz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            //Creating a producer and sending a message to the topic(test-topic)
            KafkaMessageProducer producer = new KafkaMessageProducer();
            producer.sendMessage("test-topic", "text", "Hello, Kafka!");
            producer.sendMessage("test-topic", "text", "Text1");
            producer.sendMessage("test-topic", "text", "Text2");
            producer.close();

            //Creating a Writer service to consume all the messages and write them in the db.
            //Enter your DB user and password here
            MessageWriterService service = new MessageWriterService(
                    "test-topic",
                    "jdbc:postgresql://localhost:5432/test_database",
                    "db_user",
                    "db_password"
            );
            service.start();
        } catch (Exception e) {
            logger.error("An error occurred: {}", e.getMessage(), e);
        }
    }
}