package com.example.snowflake.controller;

import com.example.snowflake.model.Customer;
import com.example.snowflake.service.SnowflakeJDBCService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;

@AllArgsConstructor
@RestController
@RequestMapping("api/v1/customers")
public class CustomerController {

    private final SnowflakeJDBCService snowflakeJDBCService;

    @GetMapping
    public List<Customer> getCustomers() throws SQLException, JsonProcessingException {

        return snowflakeJDBCService.getCustomerProcJson();

    }

}
