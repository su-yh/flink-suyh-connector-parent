package org.apache.duckdb.sink;

/**
 * @author suyh
 * @since 2026-01-09
 */

import java.io.Serializable;

/** 状态类：记录 Writer 的“记忆” (如 Checkpoint ID) */
public class DuckDBWriterState implements Serializable {
    public final long lastCheckpointId;
    public DuckDBWriterState(long lastCheckpointId) { this.lastCheckpointId = lastCheckpointId; }
}
