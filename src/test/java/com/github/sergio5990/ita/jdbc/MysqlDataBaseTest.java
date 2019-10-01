package com.github.sergio5990.ita.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MysqlDataBaseTest {

    @Test
    void database() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        assertNotNull(connection);
    }

    @Test
    void metadata() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        DatabaseMetaData metaData = connection.getMetaData();
        assertEquals("MySQL Connector/J", metaData.getDriverName());
        assertEquals("mysql-connector-java-8.0.17 (Revision: 16a712ddb3f826a1933ab42b0039f7fb9eebc6ec)", metaData.getDriverVersion());
        assertEquals("jdbc:mysql://localhost:3306/ita?logger=com.mysql.cj.log.StandardLogger&profileSQL=true",
                metaData.getURL());
        assertEquals("root@localhost", metaData.getUserName());
    }

    @Test
    void tables() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        List<String> tableNames = dataBase.getTablesMetadata(connection.getMetaData());
        System.out.println(tableNames);
    }

    @Test
    void columns() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        String columnInfo = dataBase.getColumnsMetadata(connection.getMetaData(), "salary");
        System.out.println(columnInfo);
    }

    @Test
    void statement() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        Statement statement = connection.createStatement();
        try (ResultSet rs = statement.executeQuery("select * from salary")) {
            while (rs.next()) {
                System.out.println(
                        rs.getLong("id") + "|" +
                                rs.getInt("money") + "|" +
                                rs.getString("dept"));
            }
        }
    }

    @Test
    void update() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        Statement statement = connection.createStatement();
        int updatedRows = statement.executeUpdate("update salary set money = money + 1");
        assertTrue(updatedRows > 0);
    }

    @Test
    void rsMeta() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        Connection connection = dataBase.connect();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from salary");
        ResultSetMetaData meta = rs.getMetaData();

        //Return the column count
        int columnCount = meta.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            System.out.println("Column Name: " + meta.getColumnName(i));
            System.out.println("Column Type:" + meta.getColumnType(i));
            System.out.println("Display Size: " + meta.getColumnDisplaySize(i));
            System.out.println("Precision: " + meta.getPrecision(i));
            System.out.println("Scale: " + meta.getScale(i));
            System.out.println("___________");
        }
        System.out.println("Total row count: " + meta.getColumnCount());
    }

    @Test
    void close() throws SQLException {
        // очень плохо
//        MysqlDataBase dataBase = new MysqlDataBase();
//        Connection connection = dataBase.connect();
//        Statement statement = connection.createStatement();
//        ResultSet rs = statement.executeQuery("select * from salary");

//        // плохо
//        Connection connection = null;
//        Statement statement = null;
//        ResultSet rs = null;
//        try {
//            MysqlDataBase dataBase = new MysqlDataBase();
//            connection = dataBase.connect();
//            statement = connection.createStatement();
//            rs = statement.executeQuery("select * from salary");
//        } finally {
//            if (connection != null) {
//                connection.close();
//            }
//            if (statement != null) {
//                statement.close();
//            }
//            if (rs != null) {
//                rs.close();
//            }
//        }

        //хорошо
        MysqlDataBase dataBase = new MysqlDataBase();
        try (Connection connection = dataBase.connect();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("select * from salary")) {
            assertNotNull(rs);
        }
    }

    MysqlDataBase dataBase = new MysqlDataBase();

    int findByMoneyGt(int money) throws SQLException {
        try (Connection connection = dataBase.connect();
             PreparedStatement statement = connection.prepareStatement("select money as m from salary where money > ?")) {
            statement.setInt(1, money);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    return rs.getInt("m");
                }
            }
        }
        return money;
    }

    @Test
    void preparedStatementInsert() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        try (Connection connection = dataBase.connect();
             PreparedStatement statement = connection.prepareStatement("insert into salary(dept, money) values(?,?)")) {
            statement.setString(1, "qa");
            statement.setInt(2, 50);
            statement.executeUpdate();
        }
    }

    @Test
    void preparedStatementId() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
        try (Connection connection = dataBase.connect();
             PreparedStatement statement = connection.prepareStatement("insert into salary(dept, money) values(?,?)", Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, "qa");
            statement.setInt(2, 50);
            statement.executeUpdate();

            ResultSet rs = statement.getGeneratedKeys();
            rs.next();
            long id = rs.getLong(1);

            System.out.println(id);
        }
    }


    @Test
    void noInjection() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
//        String team = "dev";
        String team = "'dev' or 1=1";
        try (Connection connection = dataBase.connect();
             PreparedStatement statement = connection.prepareStatement("select * from salary where dept = ?")) {
            statement.setString(1, team);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    System.out.println(
                            rs.getLong("id") + "|" +
                                    rs.getInt("money") + "|" +
                                    rs.getString("dept"));
                }
            }
        }
    }

    @Test
    void injection() throws SQLException {
        MysqlDataBase dataBase = new MysqlDataBase();
//        String team = "'dev'";
        String team = "'dev' or 1=1";
        try (Connection connection = dataBase.connect();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("select * from salary where dept = " + team)) {
            while (rs.next()) {
                System.out.println(
                        rs.getLong("id") + "|" +
                                rs.getInt("money") + "|" +
                                rs.getString("dept"));
            }
        }
    }
}