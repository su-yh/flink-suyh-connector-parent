package com.cdc.duckdb.config.datasource.properties;

import com.baomidou.dynamic.datasource.provider.DynamicDataSourceProvider;
import com.cdc.duckdb.config.datasource.DataSourceEnums;
import com.cdc.duckdb.config.datasource.HikariDataSourcePlus;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import javax.validation.Valid;
import java.util.HashMap;
import java.util.Map;

/**
 * @author suyh
 * @since 2024-03-20
 */
@ConfigurationProperties(prefix = "spring.datasource")
@Data
@Validated
@Slf4j
public class DynamicDataSourceProviderProperties implements DynamicDataSourceProvider {
    @Valid
    private final Map<DataSourceEnums, HikariDataSourcePlus> hikari = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {
        // 兼容配置项
        HikariDataSourcePlus cemMaster = hikari.get(DataSourceEnums.CEM_MASTER_MYSQL);
        if (cemMaster == null || !StringUtils.hasText(cemMaster.getJdbcUrl())) {
            HikariDataSourcePlus cemHistory = hikari.get(DataSourceEnums.CDS_MYSQL);
            cemMaster = cemHistory;
            hikari.put(DataSourceEnums.CEM_MASTER_MYSQL, cemMaster);
        }

        // 兼容配置项
        HikariDataSourcePlus cemSlave = hikari.get(DataSourceEnums.CEM_SLAVE_MYSQL);
        if (cemSlave == null || cemSlave.getJdbcUrl() == null) {
            cemSlave = hikari.get(DataSourceEnums.CDS_SLAVE_MYSQL);
            if (cemSlave == null || !StringUtils.hasText(cemSlave.getJdbcUrl())) {
                log.warn("CEM 没有配置从库，则与主库使用相同的数据库");
                cemSlave = cemMaster;
            }
            // HikariDataSourcePlus cemMaster = hikari.get(DataSourceEnums.CEM_MASTER_MYSQL);
            hikari.put(DataSourceEnums.CEM_SLAVE_MYSQL, cemSlave); // 没有配置从库，则与主库使用相同的数据库
        }

        // 删除过期的配置项
        hikari.remove(DataSourceEnums.CDS_MYSQL);
        hikari.remove(DataSourceEnums.CDS_SLAVE_MYSQL);


        HikariDataSourcePlus payoutMaster = hikari.get(DataSourceEnums.PAYMENT_MYSQL);
        HikariDataSourcePlus payoutSlave = hikari.get(DataSourceEnums.PAYMENT_SLAVE_MYSQL);
        if (payoutSlave == null || !StringUtils.hasText(payoutSlave.getJdbcUrl())){
            payoutSlave = payoutMaster; // 没有配置从库，则与主库使用相同的数据库
            log.warn("Payout 没有配置从库，则与主库使用相同的数据库");
            hikari.put(DataSourceEnums.PAYMENT_SLAVE_MYSQL, payoutSlave);
        }
    }

    @Override
    public synchronized Map<String, DataSource> loadDataSources() {
        Map<String, DataSource> map = new HashMap<>();
        hikari.forEach((k, v) -> map.put(k.getCode(), v));
        return map;
    }
}
