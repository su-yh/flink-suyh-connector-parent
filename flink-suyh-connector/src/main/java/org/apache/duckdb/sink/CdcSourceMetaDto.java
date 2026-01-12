package org.apache.duckdb.sink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * @author suyh
 * @since 2026-01-12
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class CdcSourceMetaDto {
    private String version; // Debezium版本
    private String connector; // 连接器类型（mysql）
    private String name; // 数据源名称
    private Long ts_ms; // 数据库端原始时间戳
    private String db; // 数据库名
    private String table; // 表名
    private String file; // binlog文件名
    private Integer pos; // binlog偏移量
    private String gtid; // GTID（开启时才有值）
    private String snapshot;
}
