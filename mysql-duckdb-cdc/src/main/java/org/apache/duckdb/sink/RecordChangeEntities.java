package org.apache.duckdb.sink;

import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import io.debezium.data.Envelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author suyh
 * @since 2026-01-17
 */
@RequiredArgsConstructor
@Slf4j
public class RecordChangeEntities {
    // 默认容量
    private static final int CAPACITY = 1000;

    private final String tableName; // duckdb 对应的表名
    private final BaseMapperDuckdb<?> baseMapperDuckdb;
    private final Map<Object, BaseEntity> upsertEntitiesMap = new ConcurrentHashMap<>();
    private final Map<Object, BaseEntity> deleteEntitiesMap = new ConcurrentHashMap<>();

    public boolean put(String op, BaseEntity entity) {
        if (op.equals(Envelope.Operation.CREATE.code()) || op.equals(Envelope.Operation.UPDATE.code())) {
            log.trace("put create|update event");
            upsertEntitiesMap.put(entity.getPrimaryKey(), entity);
            deleteEntitiesMap.remove(entity.getPrimaryKey());
        } else if (op.equals(Envelope.Operation.DELETE.code()) || op.equals(Envelope.Operation.TRUNCATE.code())) {
            log.trace("put delete|truncate event");
            deleteEntitiesMap.put(entity.getPrimaryKey(), entity);
            upsertEntitiesMap.remove(entity.getPrimaryKey());
        } else if (op.equals(Envelope.Operation.READ.code())) {
            // 全量同步阶段
            log.trace("put read event.");
            upsertEntitiesMap.put(entity.getPrimaryKey(), entity);
            deleteEntitiesMap.remove(entity.getPrimaryKey());
        }

        return upsertEntitiesMap.size() >= CAPACITY || deleteEntitiesMap.size() >= CAPACITY;
    }

    public void flush() {
        if (!upsertEntitiesMap.isEmpty()) {
            Collection<BaseEntity> entities = upsertEntitiesMap.values();
            baseMapperDuckdb.upsertObjects(entities);
            upsertEntitiesMap.clear();
        }
        if (!deleteEntitiesMap.isEmpty()) {
            Set<Object> ids = deleteEntitiesMap.keySet();
            baseMapperDuckdb.deleteBatchIds(ids);
            deleteEntitiesMap.clear();
        }
    }

    public boolean isEmpty() {
        return upsertEntitiesMap.isEmpty() && deleteEntitiesMap.isEmpty();
    }
}
