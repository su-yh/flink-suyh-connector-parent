// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.writer;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Doris Writer will load data to doris.
 *
 * @param <IN>
 */
public class DorisWriter<IN>
        implements DorisAbstractWriter<IN, DorisWriterState, DorisCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);
    private final long lastCheckpointId;
    private long curCheckpointId;
    private Map<String, DorisStreamLoad> dorisStreamLoadMap = new ConcurrentHashMap<>();
    private Map<String, LabelGenerator> labelGeneratorMap = new ConcurrentHashMap<>();
    volatile boolean globalLoading;
    private Map<String, Boolean> loadingMap = new ConcurrentHashMap<>();
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions executionOptions;
    private String labelPrefix;
    private final int subtaskId;
    private final DorisRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private BackendUtil backendUtil;
    private SinkWriterMetricGroup sinkMetricGroup;
    private Map<String, DorisWriteMetrics> sinkMetricsMap = new ConcurrentHashMap<>();
    private volatile boolean multiTableLoad = false;

    public DorisWriter(
            Sink.InitContext initContext,
            Collection<DorisWriterState> state,
            DorisRecordSerializer<IN> serializer,
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions) {
        this.lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        this.curCheckpointId = lastCheckpointId + 1;
        LOG.info("restore from checkpointId {}", lastCheckpointId);
        LOG.info("labelPrefix {}", executionOptions.getLabelPrefix());
        this.labelPrefix = executionOptions.getLabelPrefix();
        this.subtaskId = initContext.getSubtaskId();
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1,
                        r -> {
                            Thread t = new Thread(r, "stream-load-check-" + subtaskId);
                            t.setPriority(Thread.MIN_PRIORITY);
                            return t;
                        });
        this.serializer = serializer;
        if (StringUtils.isBlank(dorisOptions.getTableIdentifier())) {
            this.multiTableLoad = true;
            LOG.info("table.identifier is empty, multiple table writes.");
        }
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.executionOptions = executionOptions;
        this.globalLoading = false;
        sinkMetricGroup = initContext.metricGroup();
        initializeLoad(state);
        serializer.initial();
    }

    public void initializeLoad(Collection<DorisWriterState> state) {
        this.backendUtil = BackendUtil.getInstance(dorisOptions, dorisReadOptions, LOG);
        try {
            if (executionOptions.enabled2PC()) {
                abortLingeringTransactions(state);
            }
        } catch (Exception e) {
            LOG.error("Failed to abort transaction.", e);
            throw new DorisRuntimeException(e);
        }
    }

    @VisibleForTesting
    public void abortLingeringTransactions(Collection<DorisWriterState> recoveredStates)
            throws Exception {
        List<String> alreadyAborts = new ArrayList<>();
        // abort label in state
        for (DorisWriterState state : recoveredStates) {
            LOG.info("try to abort txn from DorisWriterState {}", state.toString());
            // Todo: When the sink parallelism is reduced,
            //  the txn of the redundant task before aborting is also needed.
            if (!state.getLabelPrefix().equals(labelPrefix)) {
                LOG.warn(
                        "Label prefix from previous execution {} has changed to {}.",
                        state.getLabelPrefix(),
                        executionOptions.getLabelPrefix());
            }
            if (state.getDatabase() == null || state.getTable() == null) {
                LOG.warn(
                        "Transactions cannot be aborted when restore because the last used flink-doris-connector version less than 1.5.0.");
                continue;
            }
            String key = state.getDatabase() + "." + state.getTable();
            DorisStreamLoad streamLoader = getStreamLoader(key);
            streamLoader.abortPreCommit(state.getLabelPrefix(), curCheckpointId);
            alreadyAborts.add(state.getLabelPrefix());
        }

        // TODO: In a multi-table scenario, if do not restore from checkpoint,
        //  when modify labelPrefix at startup, we cannot abort the previous label.
        if (!alreadyAborts.contains(labelPrefix)
                && StringUtils.isNotEmpty(dorisOptions.getTableIdentifier())
                && StringUtils.isNotEmpty(labelPrefix)) {
            // abort current labelPrefix
            DorisStreamLoad streamLoader = getStreamLoader(dorisOptions.getTableIdentifier());
            streamLoader.abortPreCommit(labelPrefix, curCheckpointId);
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        System.out.println("DorisSink 待写入原始数据：" + in); // suyh
        writeOneDorisRecord(serializer.serialize(in));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        writeOneDorisRecord(serializer.flush());
    }

    public void writeOneDorisRecord(DorisRecord record) throws IOException, InterruptedException {
        if (record == null || record.getRow() == null) {
            // ddl or value is null
            return;
        }

        // multi table load
        String tableKey = dorisOptions.getTableIdentifier();
        if (record.getTableIdentifier() != null) {
            tableKey = record.getTableIdentifier();
        }

        DorisStreamLoad streamLoader = getStreamLoader(tableKey);
        if (!loadingMap.containsKey(tableKey)) {
            // start stream load only when there has data
            LabelGenerator labelGenerator = getLabelGenerator(tableKey);
            String currentLabel = labelGenerator.generateTableLabel(curCheckpointId);
            streamLoader.startLoad(currentLabel, false);
            loadingMap.put(tableKey, true);
            globalLoading = true;
            registerMetrics(tableKey);
        }
        // System.out.println("序列化后的数据，len: " + record.getRow().length); // suyh
        // System.out.println("suyh - 序列化后的数据, database: " + record.getDatabase() + ", table: " + record.getTable() + ", row: " + new String(record.getRow())); // suyh
        streamLoader.writeRecord(record.getRow());
    }

    public void registerMetrics(String tableKey) {
        if (sinkMetricsMap.containsKey(tableKey)) {
            return;
        }
        DorisWriteMetrics metrics = DorisWriteMetrics.of(sinkMetricGroup, tableKey);
        sinkMetricsMap.put(tableKey, metrics);
    }

    @Override
    public Collection<DorisCommittable> prepareCommit() throws IOException, InterruptedException {
        // Verify whether data is written during a checkpoint
        if (!globalLoading && loadingMap.values().stream().noneMatch(Boolean::booleanValue)) {
            return Collections.emptyList();
        }
        // disable exception checker before stop load.
        globalLoading = false;
        // submit stream load http request
        List<DorisCommittable> committableList = new ArrayList<>();
        for (Map.Entry<String, DorisStreamLoad> streamLoader : dorisStreamLoadMap.entrySet()) {
            String tableIdentifier = streamLoader.getKey();
            if (!loadingMap.getOrDefault(tableIdentifier, false)) {
                LOG.debug("skip table {}, no data need to load.", tableIdentifier);
                continue;
            }
            DorisStreamLoad dorisStreamLoad = streamLoader.getValue();
            RespContent respContent = dorisStreamLoad.stopLoad();
            // refresh metrics
            if (sinkMetricsMap.containsKey(tableIdentifier)) {
                DorisWriteMetrics dorisWriteMetrics = sinkMetricsMap.get(tableIdentifier);
                dorisWriteMetrics.flush(respContent);
            }
            if (executionOptions.enabled2PC()) {
                long txnId = respContent.getTxnId();
                committableList.add(
                        new DorisCommittable(
                                dorisStreamLoad.getHostPort(), dorisStreamLoad.getDb(), txnId));
            }
        }

        // clean loadingMap
        loadingMap.clear();
        return committableList;
    }

    private void abortPossibleSuccessfulTransaction() {
        // In the case of multi-table writing, if a new table is added during the period
        // (there is no streamloader for this table in the previous Checkpoint state),
        // in the precommit phase, if some tables succeed and others fail,
        // the txn of successful precommit cannot be aborted.
        if (executionOptions.enabled2PC() && multiTableLoad) {
            LOG.info("Try to abort may have successfully preCommitted label.");
            for (Map.Entry<String, DorisStreamLoad> entry : dorisStreamLoadMap.entrySet()) {
                DorisStreamLoad abortLoader = entry.getValue();
                try {
                    abortLoader.abortTransactionByLabel(abortLoader.getCurrentLabel());
                } catch (Exception ex) {
                    LOG.warn(
                            "Skip abort transaction failed by label, reason is {}.",
                            ex.getMessage());
                }
            }
        }
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        List<DorisWriterState> writerStates = new ArrayList<>();
        for (DorisStreamLoad dorisStreamLoad : dorisStreamLoadMap.values()) {
            // Dynamic refresh backend
            dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend(subtaskId));
            DorisWriterState writerState =
                    new DorisWriterState(
                            labelPrefix,
                            dorisStreamLoad.getDb(),
                            dorisStreamLoad.getTable(),
                            subtaskId);
            writerStates.add(writerState);
        }
        this.curCheckpointId = checkpointId + 1;
        return writerStates;
    }

    private LabelGenerator getLabelGenerator(String tableKey) {
        return labelGeneratorMap.computeIfAbsent(
                tableKey,
                v ->
                        new LabelGenerator(
                                labelPrefix, executionOptions.enabled2PC(), tableKey, subtaskId));
    }

    @VisibleForTesting
    public DorisStreamLoad getStreamLoader(String tableKey) {
        LabelGenerator labelGenerator = getLabelGenerator(tableKey);
        dorisOptions.setTableIdentifier(tableKey);
        return dorisStreamLoadMap.computeIfAbsent(
                tableKey,
                v ->
                        new DorisStreamLoad(
                                backendUtil.getAvailableBackend(subtaskId),
                                dorisOptions,
                                executionOptions,
                                labelGenerator,
                                new HttpUtil(dorisReadOptions).getHttpClient()));
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close DorisWriter.");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        abortPossibleSuccessfulTransaction();

        if (dorisStreamLoadMap != null && !dorisStreamLoadMap.isEmpty()) {
            for (DorisStreamLoad dorisStreamLoad : dorisStreamLoadMap.values()) {
                dorisStreamLoad.close();
            }
        }
        serializer.close();
    }
}
