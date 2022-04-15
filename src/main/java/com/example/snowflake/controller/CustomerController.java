package com.example.snowflake.controller;

import com.example.snowflake.service.SnowflakeJDBCService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

@AllArgsConstructor
@RestController
@RequestMapping("api/v1/customers")
public class CustomerController {

    private final SnowflakeJDBCService snowflakeJDBCService;

    @GetMapping
    public String getCustommers() throws SQLException {

        return snowflakeJDBCService.getCustomerProc();

    }

}
