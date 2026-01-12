package com.cdc.duckdb.config.datasource;

import lombok.Getter;

/**
 * @author suyh
 * @since 2025-05-30
 */
@Getter
public enum DataSourceEnums {
    // 线上环境使用的是该配置项，不要删除。代码中不要使用
    @Deprecated
    CDS_MYSQL(DataSourceNames.CDS_MYSQL),
    // 线上环境使用的是该配置项，不要删除。代码中不要使用
    @Deprecated
    CDS_SLAVE_MYSQL(DataSourceNames.CDS_SLAVE_MYSQL),

    CEM_MASTER_MYSQL(DataSourceNames.CEM_MASTER_MYSQL),
    CEM_SLAVE_MYSQL(DataSourceNames.CEM_SLAVE_MYSQL),
    PAYMENT_MYSQL(DataSourceNames.PAYMENT_MYSQL),
    PAYMENT_SLAVE_MYSQL(DataSourceNames.PAYMENT_SLAVE_MYSQL),

    ;

    private final String code;

    DataSourceEnums(String code) {
        this.code = code;
    }
}
