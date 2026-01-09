package org.apache.duckdb.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class DuckDBCommitter implements Committer<DuckDBCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(DuckDBCommitter.class);

    @Override
    public void commit(Collection<CommitRequest<DuckDBCommittable>> requests) {
        for (CommitRequest<DuckDBCommittable> request : requests) {
            LOG.info("二阶段提交 - 第二阶段 (Commit): 真正提交事务 {}", request.getCommittable().txId);
        }
    }

    @Override
    public void close() {}
}