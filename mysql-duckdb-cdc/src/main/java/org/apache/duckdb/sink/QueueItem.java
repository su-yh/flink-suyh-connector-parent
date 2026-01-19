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
public class QueueItem {
    public static final QueueItem INSTANCE = new QueueItem();

    private QueueItem() {
    }

    // key: duckdb 表名
    private final Map<String, RecordChangeEntities> tableEntitiesMapping = new HashMap<>();

    public void register(String tbName, BaseMapperDuckdb<?> baseMapperDuckdb) {
        RecordChangeEntities recordChangeEntities = new RecordChangeEntities(tbName, baseMapperDuckdb);
        tableEntitiesMapping.put(tbName, recordChangeEntities);
    }

    // 返回对应表的队列是否满
    public boolean put(String op, String tbName, BaseEntity entity) {
        RecordChangeEntities recordChangeEntities = tableEntitiesMapping.get(tbName);
        if (recordChangeEntities == null) {
            log.warn("CANNOT FOUND table: {}", tbName);
            return false;
        }

        return recordChangeEntities.put(op, entity);
    }

    public boolean isEmpty() {
        for (Map.Entry<String, RecordChangeEntities> entry : tableEntitiesMapping.entrySet()) {
            RecordChangeEntities recordChange = entry.getValue();
            if (recordChange != null && !recordChange.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void flush() {
        tableEntitiesMapping.forEach((tb, record) -> record.flush());
    }
}
