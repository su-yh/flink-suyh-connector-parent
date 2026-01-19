package org.apache.duckdb.sink;

import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @author suyh
 * @since 2026-01-15
 */
@Slf4j
public class TableRecordBuffer {
    // TableRecordBuffer 只有一个实例，且约定在使用的时候同一时刻只能有一个线程持有，减少了锁的问题。
    // 但是为了避免后期维护遗漏该约定，当前类的方法全部都添加了 synchronized 同步关键字
    public static final TableRecordBuffer INSTANCE = new TableRecordBuffer();

    private TableRecordBuffer() {
    }

    // key: duckdb 表名
    private final Map<String, TableChangeRecorder> tableEntitiesMapping = new HashMap<>();

    public synchronized void register(String duckdbTableName, BaseMapperDuckdb<?> baseMapperDuckdb) {
        TableChangeRecorder tableChangeRecorder = new TableChangeRecorder(duckdbTableName, baseMapperDuckdb);
        tableEntitiesMapping.put(duckdbTableName, tableChangeRecorder);
    }

    // 返回对应表的队列是否满
    public synchronized boolean put(String op, String tbName, BaseEntity entity) {
        TableChangeRecorder tableChangeRecorder = tableEntitiesMapping.get(tbName);
        if (tableChangeRecorder == null) {
            log.warn("CANNOT FOUND table: {}", tbName);
            return false;
        }

        return tableChangeRecorder.put(op, entity);
    }

    public synchronized boolean isEmpty() {
        for (Map.Entry<String, TableChangeRecorder> entry : tableEntitiesMapping.entrySet()) {
            TableChangeRecorder recordChange = entry.getValue();
            if (recordChange != null && !recordChange.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public synchronized void flush() {
        tableEntitiesMapping.forEach((tb, record) -> record.flush());
    }
}
