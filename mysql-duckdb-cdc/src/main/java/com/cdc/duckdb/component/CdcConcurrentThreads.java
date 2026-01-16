package com.cdc.duckdb.component;

import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.duckdb.sink.TableRecordBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author suyh
 * @since 2026-01-16
 */
@RequiredArgsConstructor
@Slf4j
public class CdcConcurrentThreads {
    private final Function<String, BaseMapperDuckdb<?>> obtainMapperCallback;
    private ArrayBlockingQueue<TableRecordBuffer> writeQueue;
    private DuckdbWriterThread duckdbWriterThread;
    private ScheduledExecutorService scheduledExecutor;

    public synchronized void init() {
        if (writeQueue == null) {
            writeQueue = new ArrayBlockingQueue<>(1);   // 只能一个元素
            writeQueue.add(new TableRecordBuffer(1000));
        }
        if (duckdbWriterThread == null) {
            duckdbWriterThread = new DuckdbWriterThread();
            duckdbWriterThread.start();
        }
        if (scheduledExecutor == null) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutor.scheduleAtFixedRate(this::flushTimer, 30, 1, TimeUnit.SECONDS);
        }
    }

    public synchronized void stop() {
        if (writeQueue != null) {
            // TODO: suyh - 待实现
            // ... 其他销毁工作


            // ######################
            writeQueue = null;
        }

        if (duckdbWriterThread != null) {
            // TODO: suyh - 待实现
            // ... 其他销毁工作

            // ######################
            duckdbWriterThread = null;
        }

        if (scheduledExecutor != null) {
            // TODO: suyh - 待实现
            // ... 其他销毁工作

            // ######################
            scheduledExecutor = null;
        }

    }

    // 提供给flink sink 调用
    public void write(String tbName, Object entity) throws InterruptedException {
        log.trace("write, table name: {}, start...",  tbName);
        TableRecordBuffer tableRecordBuffer = writeQueue.take();
        log.trace("write, table name: {}, finished",  tbName);
        boolean full = tableRecordBuffer.put(tbName, entity);
        if (full) {
            doFlush(tableRecordBuffer);
        } else {
            writeQueue.put(tableRecordBuffer);
        }
    }

    private void flushTimer() {
        // TODO: suyh - if 未超时 return;
        flush();
    }

    public void flush() {
        try {
            TableRecordBuffer tableRecordBuffer = writeQueue.poll();
            if (tableRecordBuffer != null) {
                if (!tableRecordBuffer.isEmpty()) {
                    doFlush(tableRecordBuffer);
                } else {
                    // 将空buffer 还回去
                    writeQueue.put(tableRecordBuffer);
                    log.trace("将空buffer 还回写队列");
                }
            }
        } catch (Exception e) {
            log.error("flushTimer error.", e);
        }
    }

    private void doFlush(TableRecordBuffer tableRecordBuffer) throws InterruptedException {
        duckdbWriterThread.write(tableRecordBuffer);
    }

    private class DuckdbWriterThread extends Thread {
        private final ArrayBlockingQueue<TableRecordBuffer> readQueue = new ArrayBlockingQueue<>(1);

        @Override
        public void run() {
            while (true) {
                try {
                    TableRecordBuffer tableRecordBuffer = readQueue.poll(1, TimeUnit.SECONDS);
                    if (tableRecordBuffer != null) {
                        log.trace("从读队列中取到buffer");
                        tableRecordBuffer.upsertEntitiesAndReset(obtainMapperCallback);

                        tableRecordBuffer.reset();
                        writeQueue.put(tableRecordBuffer);
                        log.trace("buffer 处理完，还回写队列");
                    }
                } catch (InterruptedException e) {
                    log.error("{} error.", DuckdbWriterThread.class.getSimpleName(), e);
                }
            }
        }

        public void write(TableRecordBuffer tableRecordBuffer) throws InterruptedException {
            readQueue.put(tableRecordBuffer);
        }
    }
}
