package com.cdc.duckdb.component;

import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.duckdb.sink.TableRecordBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author suyh
 * @since 2026-01-16
 */
@RequiredArgsConstructor
@Slf4j
public class CdcConcurrentThreads {
    private ArrayBlockingQueue<TableRecordBuffer> writeQueue;
    private DuckdbWriterThread duckdbWriterThread;
    private ScheduledExecutorService scheduledExecutor;

    public synchronized void init() {
        if (writeQueue == null) {
            writeQueue = new ArrayBlockingQueue<>(1);   // 有且只能一个元素
            writeQueue.add(TableRecordBuffer.INSTANCE);
        }
        if (duckdbWriterThread == null) {
            duckdbWriterThread = new DuckdbWriterThread();
            duckdbWriterThread.start();
        }
        if (scheduledExecutor == null) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutor.scheduleAtFixedRate(this::flushTimer, 10, 1, TimeUnit.SECONDS);
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
    public void write(String op, String tbName, BaseEntity entity) throws InterruptedException {
        log.trace("write, table name: {}, start...",  tbName);
        TableRecordBuffer tableRecordBuffer = writeQueue.take();
        log.trace("write, table name: {}, finished",  tbName);
        boolean full = tableRecordBuffer.put(op, tbName, entity);
        if (full) {
            doFlush(tableRecordBuffer);
        } else {
            restoreBuffer(tableRecordBuffer);
        }
    }

    private void flushTimer() {
        // TODO: suyh - if 未超时 return;
        flush();
    }

    public void syncFlush() {
        TableRecordBuffer tableRecordBuffer = takeBuffer();
        tableRecordBuffer.flush();
        restoreBuffer(tableRecordBuffer);
    }

    private TableRecordBuffer takeBuffer() {
        int count = 3;
        for (int i = 0; i < count; i++) {
            try {
                return writeQueue.take();
            } catch (InterruptedException e) {
                log.warn("writeQueue.take failed, retry {}/{}", (i + 1), count, e);
            }
        }

        throw new RuntimeException("takeBuffer failed.");
    }

    private void flush() {
        try {
            TableRecordBuffer tableRecordBuffer = writeQueue.poll();
            if (tableRecordBuffer != null) {
                if (!tableRecordBuffer.isEmpty()) {
                    doFlush(tableRecordBuffer);
                } else {
                    log.trace("将空buffer 还回写队列");
                    restoreBuffer(tableRecordBuffer);
                }
            }
        } catch (Exception e) {
            log.error("flushTimer error.", e);
        }
    }

    private void doFlush(TableRecordBuffer tableRecordBuffer) throws InterruptedException {
        duckdbWriterThread.write(tableRecordBuffer);
    }

    private void restoreBuffer(TableRecordBuffer tableRecordBuffer) {
        int count = 3;
        for (int i = 0; i < count; i++) {
            try {
                writeQueue.put(tableRecordBuffer);
                return;
            } catch (InterruptedException e) {
                log.warn("writeQueue.put failed, retry {}/{}", (i + 1), count, e);
            }
        }

        throw new RuntimeException("recycleBuffer failed.");
    }

    public void register(String tableName, BaseMapperDuckdb<?> baseMapperBean) {
        int count = 1000;
        for (int i = 0; i < count; i++) {
            try {
                TableRecordBuffer tableRecordBuffer = writeQueue.take();
                tableRecordBuffer.register(tableName, baseMapperBean);
                restoreBuffer(tableRecordBuffer);
                return;
            } catch (InterruptedException e) {
                log.warn("register table({}) failed, retry {}/{}", tableName, (i + 1), count, e);
            }
        }
    }


    // ####################################################################################
    private class DuckdbWriterThread extends Thread {
        private final ArrayBlockingQueue<TableRecordBuffer> readQueue = new ArrayBlockingQueue<>(1);

        @Override
        public void run() {
            while (true) {
                try {
                    TableRecordBuffer tableRecordBuffer = readQueue.poll(1, TimeUnit.SECONDS);
                    if (tableRecordBuffer != null) {
                        log.trace("从读队列中取到buffer");
                        tableRecordBuffer.flush();

                        restoreBuffer(tableRecordBuffer);
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
