package org.apache.duckdb.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.Collections;

@Slf4j
public class DuckDBSink implements
        TwoPhaseCommittingSink<RecordDto, DuckDBCommittable>,
        StatefulSink<RecordDto, DuckDBWriterState> {

    // suyh - 首次运行，也就是说没有从checkpoint 启动。
    @Override // 满足 StatefulSink
    public DuckDBWriter createWriter(InitContext context) {
        log.info("suyh - createWriter");
        return new DuckDBWriter(Collections.emptyList());
    }

    // suyh - 非首次运行，也就是说从checkpoint 启动
    @Override // 满足 StatefulSink 的恢复路径
    public StatefulSinkWriter<RecordDto, DuckDBWriterState> restoreWriter(
            InitContext context, Collection<DuckDBWriterState> recoveredState) {
        log.info("suyh - restoreWriter");
        return new DuckDBWriter(recoveredState);
    }

    @Override // 满足 TwoPhaseCommittingSink
    public Committer<DuckDBCommittable> createCommitter() {
        return new DuckDBCommitter();
    }

    @Override
    public SimpleVersionedSerializer<DuckDBCommittable> getCommittableSerializer() {
        return new SimpleVersionedSerializer<DuckDBCommittable>() {
            @Override public int getVersion() { return 1; }
            @Override public byte[] serialize(DuckDBCommittable obj) { return obj.txId.getBytes(); }
            @Override public DuckDBCommittable deserialize(int version, byte[] serialized) {
                return new DuckDBCommittable(new String(serialized));
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<DuckDBWriterState> getWriterStateSerializer() {
        return new SimpleVersionedSerializer<DuckDBWriterState>() {
            @Override public int getVersion() { return 1; }
            @Override public byte[] serialize(DuckDBWriterState obj) {
                return java.nio.ByteBuffer.allocate(8).putLong(obj.lastCheckpointId).array();
            }
            @Override public DuckDBWriterState deserialize(int version, byte[] serialized) {
                return new DuckDBWriterState(java.nio.ByteBuffer.wrap(serialized).getLong());
            }
        };
    }
}