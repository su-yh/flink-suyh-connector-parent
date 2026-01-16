package org.apache.duckdb.sink;

import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author suyh
 * @since 2026-01-15
 */
@Slf4j
public class TableRecordBuffer {
    private final int capacity;
    // key: duckdb 表名  key: 主键
    private final Map<String, Map<Object, BaseEntity>> tableEntitiesMapping = new HashMap<>();

    public TableRecordBuffer(int capacity) {
        Assert.isTrue(capacity > 0 && capacity <= 5000, "capacity max 5000");
        this.capacity = capacity;
    }

    // 返回对应表的队列是否满
    public boolean put(String tbName, BaseEntity entity) {
        Map<Object, BaseEntity> entityMap = tableEntitiesMapping.computeIfAbsent(tbName, k -> new HashMap<>(capacity));
        entityMap.put(entity.getPrimaryKey(), entity);

        return capacity <= entityMap.size();
    }

    public boolean isEmpty() {
        AtomicBoolean emptyFlag = new AtomicBoolean(true);

        tableEntitiesMapping.forEach((tableName, entitiesMap) -> {
            if (emptyFlag.get() && !entitiesMap.isEmpty()) {
                emptyFlag.set(false);
            }
        });

        return emptyFlag.get();
    }

    public void upsertEntitiesAndReset(Function<String, BaseMapperDuckdb<?>> obtainMapperCallback) {
        tableEntitiesMapping.forEach((tableName, entitiesMap) -> {
            if (entitiesMap == null || entitiesMap.isEmpty()) {
                return;
            }

            BaseMapperDuckdb<?> baseMapperDuckdb = obtainMapperCallback.apply(tableName);
            if (baseMapperDuckdb == null) {
                log.error("cannot found {}, by table name: {}", BaseMapperDuckdb.class.getSimpleName(), tableName);
                return;
            }

            Collection<BaseEntity> entities = entitiesMap.values();

            long beforeTs = System.currentTimeMillis();
            baseMapperDuckdb.upsertObjects(entities);
            // baseMapperDuckdb.insertObjects(entities);
            long lastTs = System.currentTimeMillis();
            log.info("upsertEntities, table name: {}, size: {}, time: {}ms", tableName, entities.size(), (lastTs - beforeTs));
            entitiesMap.clear();
        });
    }
}
