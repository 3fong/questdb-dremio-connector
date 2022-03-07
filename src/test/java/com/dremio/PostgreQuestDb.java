package com.dremio;

import java.sql.*;
import java.util.Properties;

public class PostgreQuestDb {
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");

        final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:8812/", properties);
        Statement statement = connection.createStatement();
        boolean execute = statement.execute("demoapp limit 1");
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("id")+" " +resultSet.getString("name"));
        }
        System.out.println("Connected");
        connection.close();
    }
}
