package com.haykz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBHandlerService {
    private Connection connection;
    private static final Logger logger = LoggerFactory.getLogger(DBHandlerService.class);

    public DBHandlerService(String url, String user, String password) throws SQLException {
        connection = DriverManager.getConnection(url, user, password);
    }

    public void insertIntoDB(String message) throws SQLException {
        String query = "INSERT INTO message (text) VALUES (?)";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, message);
            stmt.executeUpdate();
        } catch (Exception e) {
            logger.error("An error occurred: {}", e.getMessage(), e);
        }
    }
}
