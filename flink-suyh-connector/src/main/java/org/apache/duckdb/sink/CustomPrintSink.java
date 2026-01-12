package org.apache.duckdb.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author suyh
 * @since 2026-01-12
 */
public class CustomPrintSink extends RichSinkFunction<RecordDto> {
    // 自定义日期格式化器（日志常用时间戳）
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    // 可选：自定义打印前缀（区分不同数据流）
    private String logPrefix;
    // 可选：Flink运行时上下文（获取任务信息）
    private RuntimeContext runtimeContext;

    // 构造方法，支持传入自定义前缀
    public CustomPrintSink() {
        this("CustomPrint");
    }

    public CustomPrintSink(String logPrefix) {
        this.logPrefix = logPrefix;
    }

    // 初始化方法，获取运行时上下文（任务ID、并行度等）
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.runtimeContext = getRuntimeContext();
    }

    // 核心方法：处理每条数据，实现自定义打印格式
    @Override
    public void invoke(RecordDto recordDto, Context context) throws Exception {
        // 1. 非空校验（避免之前的NullPointerException）
        if (recordDto == null) {
            return;
        }

        // 2. 构造自定义打印内容（类似日志格式：时间 + 前缀 + 任务信息 + 数据内容）
        String currentTime = LocalDateTime.now().format(DATE_FORMATTER);
        int taskId = runtimeContext.getIndexOfThisSubtask(); // 任务ID
        int parallelism = runtimeContext.getNumberOfParallelSubtasks(); // 并行度
        String customLog = String.format(
                "[%s] [%s] [Task-%d/%d] 数据内容。op: %s, before: %s, after: %s",
                currentTime, logPrefix, taskId + 1, parallelism, recordDto.getOperation(), recordDto.getBefore(), recordDto.getAfter()
        );

        // 3. 输出到控制台（模仿print()，也可输出到文件）
        System.out.println(customLog);
        // 可选：输出到标准错误流（区分普通日志和错误日志）
        // System.err.println(customLog);
    }
}
