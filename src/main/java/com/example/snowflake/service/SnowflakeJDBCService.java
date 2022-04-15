package com.example.snowflake.service;

import com.example.snowflake.model.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.List;
import java.util.Properties;

@Service
public class SnowflakeJDBCService {

    public List<Customer> getCustomerProcJson() throws SQLException, JsonProcessingException {
        Connection connection = getConnection();

        PreparedStatement statement = (PreparedStatement)
                connection.prepareStatement("CALL SP_GET_CUSTOMER_JSON(60001)");

        ResultSet resultSet = statement.executeQuery();

        if (resultSet.next()) {
            System.out.println(resultSet.getString(1));
            return convertToCustomer(resultSet.getString(1));
        }
        resultSet.close();
        statement.close();
        connection.close();

        return List.of();
    }

    private List<Customer> convertToCustomer(String customerData) throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readValue(customerData, new TypeReference<List<Customer>>() {});
    }

    public String getCustomerProc() throws SQLException {
        Connection connection = getConnection();

        PreparedStatement statement = (PreparedStatement)
                connection.prepareStatement("CALL SP_GET_CUSTOMER(60001)");

        ResultSet resultSet = statement.executeQuery();

        int rowNum = 0;
        String ret = "";
        while (resultSet.next()) {
            System.out.println("row " + rowNum + ", column 0: " + resultSet.getString(1));
            ret = ret + "row " + rowNum + ", column 0: " + resultSet.getString(1);
        }
        resultSet.close();
        statement.close();
        connection.close();

        return ret;
    }

    public String getCustomer() throws SQLException {
        // get connection
        System.out.println("Create JDBC connection");
        Connection connection = getConnection();
        System.out.println("Done creating JDBC connection\n");

        // create statement
        System.out.println("Create JDBC statement");
        Statement statement = connection.createStatement();
        System.out.println("Done creating JDBC statement\n");

        // query the data
        System.out.println("Query customers");
        ResultSet resultSet = statement.executeQuery("select c_custkey, c_name, c_address from customer where c_custkey = 60001");
        System.out.println("Metadata:");
        System.out.println("================================");

        // fetch metadata
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        System.out.println("Number of columns=" + resultSetMetaData.getColumnCount());
        for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
            System.out.println(
                    "Column " + colIdx + ": type=" + resultSetMetaData.getColumnTypeName(colIdx + 1));
        }

        // fetch data
        System.out.println("\nData:");
        System.out.println("================================");
        int rowIdx = 0;
        while (resultSet.next()) {
            System.out.println("row " + 0 + ", column 0: " + resultSet.getString(1));
            System.out.println("row " + 1 + ", column 1: " + resultSet.getString(2));
            System.out.println("row " + 2 + ", column 2: " + resultSet.getString(3));
        }
        resultSet.close();
        statement.close();
        connection.close();
        return "Sucess";
    }

    private static Connection getConnection() throws SQLException {

        // build connection properties
        Properties properties = new Properties();
        properties.put("user", "luispaolini"); // replace "" with your user name
        properties.put("password", "#Nr43snowflake"); // replace "" with your password
        properties.put("warehouse", "DEMO"); // replace "" with target warehouse name
        properties.put("db", "DEMO_DATA"); // replace "" with target database name
        properties.put("schema", "DEMO"); // replace "" with target schema name
        // properties.put("tracing", "all"); // optional tracing property

        // Replace <account_identifier> with your account identifier. See
        // https://docs.snowflake.com/en/user-guide/admin-account-identifier.html
        // for details.
        String connectStr = "jdbc:snowflake://https://wy30651.us-east-2.aws.snowflakecomputing.com/";
        return DriverManager.getConnection(connectStr, properties);
    }

}
