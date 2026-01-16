package org.apache.duckdb.sink;

import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author suyh
 * @since 2026-01-15
 */
@Slf4j
public class TableRecordBuffer {
    private final int capacity;
    // key: duckdb 表名
    private final Map<String, List<Object>> tableEntitiesMapping = new HashMap<>();

    public TableRecordBuffer(int capacity) {
        Assert.isTrue(capacity > 0 && capacity <= 5000, "capacity max 5000");
        this.capacity = capacity;
    }

    // 返回对应表的队列是否满
    public boolean put(String tbName, Object entity) {
        List<Object> entities = tableEntitiesMapping.computeIfAbsent(tbName, k -> new ArrayList<>(capacity));
        entities.add(entity);

        return capacity <= entities.size();
    }

    public boolean isEmpty() {
        Collection<List<Object>> values = tableEntitiesMapping.values();
        for (List<Object> list : values) {
            if (list != null && !list.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void upsertEntitiesAndReset(Function<String, BaseMapperDuckdb<?>> obtainMapperCallback) {
        tableEntitiesMapping.forEach((tableName, entities) -> {
            if (entities == null || entities.isEmpty()) {
                return;
            }

            BaseMapperDuckdb<?> baseMapperDuckdb = obtainMapperCallback.apply(tableName);
            if (baseMapperDuckdb == null) {
                log.error("cannot found {}, by table name: {}", BaseMapperDuckdb.class.getSimpleName(), tableName);
                return;
            }

            // 这里不用管分批，因为capacity 已经限制了。
            baseMapperDuckdb.upsertObjects(entities);
            entities.clear();
        });
    }

    public void reset() {
        tableEntitiesMapping.forEach((k, v) -> v.clear());
    }
}
