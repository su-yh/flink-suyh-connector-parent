package org.apache.duckdb.sink;

import com.cdc.duckdb.component.DuckdbMapperManagerComponent;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DuckDBWriter implements
        StatefulSink.StatefulSinkWriter<RecordDto, DuckDBWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<RecordDto, DuckDBCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DuckDBWriter.class);
    private long lastCkId = 0;
    // TODO: suyh - 这个对象，该如何传入？
    public static DuckdbMapperManagerComponent duckdbMapperManagerComponent;

    public DuckDBWriter(Iterable<DuckDBWriterState> states) {
        // 模拟恢复逻辑
        for (DuckDBWriterState state : states) {
            this.lastCkId = state.lastCheckpointId;
            LOG.debug("检测到恢复状态，从 Checkpoint {} 恢复中...", lastCkId);
        }
    }

    @Override
    public void write(RecordDto recordDto, Context context) throws IOException {
        LOG.debug("接收到数据 (准备写入缓存): {}", recordDto);
        LOG.debug("接收到数据 (准备写入缓存)，suyh - database: {}, table: {}",
                recordDto.getSource().getDb(), recordDto.getSource().getTable());
        // TODO: suyh - 待实现！！！
        try {
            duckdbMapperManagerComponent.upsertEntity(recordDto);
        } catch (InstantiationException | IllegalAccessException e) {
            LOG.error("upsertEntity failed.", e);
            throw new RuntimeException(e);
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
    public void flush(boolean endOfInput) {}

    @Override
    public void close() throws Exception {
        LOG.info("关闭 Writer 资源");
    }
}
