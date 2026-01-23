package org.apache.duckdb.sink;

import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.SQLParserSchemaChange;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author suyh
 * @since 2026-01-17
 */
@Slf4j
public class TableChangeRecorder {
    // 默认容量
    private static final int CAPACITY = 1000;

    private final String duckdbTableName; // duckdb 对应的表名
    private final BaseMapperDuckdb<?> baseMapperDuckdb;
    private final SQLParserSchemaChange schemaChange;
    private final Map<Object, BaseEntity> upsertEntitiesMap = new ConcurrentHashMap<>();
    private final Map<Object, BaseEntity> deleteEntitiesMap = new ConcurrentHashMap<>();

    public TableChangeRecorder(String duckdbTableName, BaseMapperDuckdb<?> baseMapperDuckdb) {
        this.duckdbTableName = duckdbTableName;
        this.baseMapperDuckdb = baseMapperDuckdb;
        this.schemaChange = new SQLParserSchemaChange(duckdbTableName);
    }

    // 返回对应表的队列是否满
    public boolean put(String op, BaseEntity entity) {
        if (op.equals(Envelope.Operation.CREATE.code()) || op.equals(Envelope.Operation.UPDATE.code())) {
            log.trace("put create|update event");
            upsertEntitiesMap.put(entity.getPrimaryKey(), entity);
            deleteEntitiesMap.remove(entity.getPrimaryKey());
        } else if (op.equals(Envelope.Operation.DELETE.code())) {
            log.trace("put delete|truncate event");
            deleteEntitiesMap.put(entity.getPrimaryKey(), entity);
            upsertEntitiesMap.remove(entity.getPrimaryKey());
        } else if (op.equals(Envelope.Operation.READ.code())) {
            // 全量同步阶段，也就是首次执行，或者叫没有从checkpoint 启动
            log.trace("put read event.");
            upsertEntitiesMap.put(entity.getPrimaryKey(), entity);
            deleteEntitiesMap.remove(entity.getPrimaryKey());
        } else {
            log.warn("Unsupported op: " + op);
            return false;
        }

        return upsertEntitiesMap.size() >= CAPACITY || deleteEntitiesMap.size() >= CAPACITY;
    }

    public void flush() {
        if (!upsertEntitiesMap.isEmpty()) {
            long start = System.currentTimeMillis();
            Collection<BaseEntity> entities = upsertEntitiesMap.values();
            int size = entities.size();
            baseMapperDuckdb.upsertObjects(entities);
            upsertEntitiesMap.clear();
            long last = System.currentTimeMillis();
            log.debug("duckdb table name: {}, upsert entities size: {}, duration: {}ms", duckdbTableName, size, (last - start));
        }
        if (!deleteEntitiesMap.isEmpty()) {
            long start = System.currentTimeMillis();
            Set<Object> ids = deleteEntitiesMap.keySet();
            int size = ids.size();
            baseMapperDuckdb.deleteBatchIds(ids);
            deleteEntitiesMap.clear();
            long last = System.currentTimeMillis();
            log.debug("duckdb table name: {}, delete entities size: {}, duration: {}ms", duckdbTableName, size, (last - start));
        }
    }

    public boolean isEmpty() {
        return upsertEntitiesMap.isEmpty() && deleteEntitiesMap.isEmpty();
    }

    public void ddlAlter(JsonNode historyRecord) {
        // 参考：org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer.initSchemaChangeInstance
        // 使用 SQLParserSchemaChange
        // DDL 入口：org.apache.doris.flink.sink.writer.serializer.jsondebezium.SQLParserSchemaChange.schemaChange

        List<String> ddlList = schemaChange.tryParseAlterDDLs(historyRecord);
        if (ddlList == null || ddlList.isEmpty()) {
            log.warn("ddl list is empty. historyRecord: {}", historyRecord.toString());
            return;
        }

        for (String ddl : ddlList) {
            baseMapperDuckdb.executeDdlSql(ddl);
        }
    }
}
