package com.cdc.duckdb;

import com.cdc.duckdb.component.CdcRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author suyh
 * @since 2026-01-12
 */
@SpringBootApplication
public class MysqlDuckdbCdcApplication {
    public static void main(String[] args) {
        CdcRunner.args = args;
        SpringApplication.run(MysqlDuckdbCdcApplication.class);
    }
}
