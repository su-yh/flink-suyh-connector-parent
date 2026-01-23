package org.apache.duckdb.sink;

import com.cdc.duckdb.component.DuckdbMapperManagerComponent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.doris.flink.sink.writer.EventType;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumSchemaChange;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DuckDBWriter implements
        StatefulSink.StatefulSinkWriter<RecordDto, DuckDBWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<RecordDto, DuckDBCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DuckDBWriter.class);
    // TODO: suyh - 这个对象，该如何传入比较合适？
    public static DuckdbMapperManagerComponent duckdbMapperManagerComponent;

    private long lastCkId = 0;

    public DuckDBWriter(Iterable<DuckDBWriterState> states) {
        // 恢复逻辑
        for (DuckDBWriterState state : states) {
            this.lastCkId = state.lastCheckpointId;
            LOG.debug("检测到恢复状态，从 Checkpoint {} 恢复中...", lastCkId);
        }
    }

    @Override
    public void write(RecordDto recordDto, Context context) throws IOException, InterruptedException {
        String op = recordDto.getOperation();
        if (!StringUtils.hasText(op)) {
            // 参考：org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer.serialize 的判断
            // 只要op 为null 就是ddl 操作

            String historyRecordJson = recordDto.getHistoryRecordJson();
            if (!StringUtils.hasText(historyRecordJson)) {
                LOG.warn("historyRecordJson is empty");
                return;
            }

            JsonNode historyRecord = JsonUtils.deserializeToJsonNode(historyRecordJson);
            EventType eventType = JsonDebeziumSchemaChange.extractEventType(historyRecord);
            if (eventType == null) {
                JsonNode ddlNode = historyRecord.get("ddl");
                if (ddlNode == null || ddlNode instanceof NullNode) {
                    LOG.warn("Failed to parse ddl, ddl json node is empty. historyRecord={}", historyRecordJson);
                    return;
                }

                String ddlText = ddlNode.asText();
                if (!StringUtils.hasText(ddlText)) {
                    LOG.warn("Failed to parse ddl, ddl text is empty. historyRecord={}", historyRecordJson);
                    return;
                }
                try {
                    Statement statement = CCJSqlParserUtil.parse(ddlText);
                    if (!(statement instanceof Truncate)) {
                        LOG.warn("Unsupported ddl operations, ddl={}", ddlText);
                        return;
                    }

                    duckdbMapperManagerComponent.ddlTruncate(recordDto.getSource().getTable());
                } catch (JSQLParserException e) {
                    LOG.warn("Failed to parse DDL SQL, SQL={}", ddlText, e);
                }
            } else if (eventType.equals(EventType.CREATE)) {
                LOG.info("IGNORE CREATE TABLE, table name: {}", recordDto.getSource().getTable());
            } else if (eventType.equals(EventType.ALTER)) {
                duckdbMapperManagerComponent.ddlAlter(recordDto.getSource().getTable(), historyRecord);
            }
        } else {
            duckdbMapperManagerComponent.write(recordDto);
        }
    }

    @Override
    public List<DuckDBCommittable> prepareCommit() throws IOException {
        String mockTxId = "TX-" + System.currentTimeMillis();
        LOG.debug("二阶段提交 - 第一阶段 (Prepare): 生成提交指令 {}", mockTxId);
        return Collections.singletonList(new DuckDBCommittable(mockTxId));
    }

    @Override
    public List<DuckDBWriterState> snapshotState(long checkpointId) throws IOException {
        LOG.debug("状态快照 - 记录 Checkpoint ID: {}", checkpointId);
        return Collections.singletonList(new DuckDBWriterState(checkpointId));
    }

    @Override
    public void flush(boolean endOfInput) {
        LOG.info("checkpoint flush triggered.");
        duckdbMapperManagerComponent.syncFlush();
    }

    @Override
    public void close() throws Exception {
        LOG.info("关闭 Writer 资源");
    }
}
