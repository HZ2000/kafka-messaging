package com.haykz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class MessageWriterService {
    private KafkaService consumer;
    private DBHandlerService dbHandler;
    private static final Logger logger = LoggerFactory.getLogger(MessageWriterService.class);

    public MessageWriterService(String kafkaTopic, String dbUrl, String dbUser, String dbPassword) throws Exception {
        consumer = new KafkaService(kafkaTopic);
        dbHandler = new DBHandlerService(dbUrl, dbUser, dbPassword);
    }

    public void start() {
        consumer.consumeMessages(message -> {
            try {
                dbHandler.insertIntoDB(message);
            } catch (SQLException e) {
                logger.error("An error occurred: {}", e.getMessage(), e);
            }
        });
    }
}
