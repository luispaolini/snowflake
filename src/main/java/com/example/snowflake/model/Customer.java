package com.example.snowflake.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Customer {
    @JsonAlias({ "C_CUSTKEY" })
    private Long cCustkey;

    @JsonAlias({ "C_NAME" })
    private String cName;

    @JsonAlias({ "C_ADDRESS" })
    private String cAddress;
}
