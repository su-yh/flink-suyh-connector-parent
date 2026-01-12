package org.apache.duckdb.sink;

/**
 * @author suyh
 * @since 2026-01-09
 */

import java.io.Serializable;

/** 提交类：记录 Writer 交给 Committer 的“指令” (如 事务ID) */
public class DuckDBCommittable implements Serializable {
    public final String txId;
    public DuckDBCommittable(String txId) { this.txId = txId; }
}
