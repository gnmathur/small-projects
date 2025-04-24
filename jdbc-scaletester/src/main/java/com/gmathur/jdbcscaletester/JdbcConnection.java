package com.gmathur.jdbcscaletester;

import java.sql.*;
import java.util.Properties;

public class JdbcConnection {
    private final Connection conn;

    public JdbcConnection(final String url, final String username, final String password, final Boolean isSsl) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Properties props = new Properties();
        props.setProperty("user", username);
        //props.setProperty("password", password);
        try {
            conn = DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        System.out.println("Connected");
    }

    public Connection connection() {
        return conn;
    }

    public Statement statement() throws SQLException {
        return conn.createStatement();
    }

}
