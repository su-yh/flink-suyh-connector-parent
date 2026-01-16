package com.cdc.duckdb.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.apache.duckdb.sink.DuckdbDatabaseSync;
import org.apache.duckdb.sink.DuckdbMysqlDatabaseSync;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author suyh
 * @since 2026-01-12
 */
@Component
@Slf4j
public class CdcRunner implements ApplicationRunner {
    public static String[] args = null;

    private static final List<String> EMPTY_KEYS =
            Collections.singletonList(DatabaseSyncConfig.PASSWORD);

    private JobClient jobClient;

    @Resource
    private DuckdbMapperManagerComponent duckdbMapperManagerComponent;

    @Override
    public void run(ApplicationArguments appArgs) throws Exception {
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        jobClient = createMySQLSyncDuckdb(params, this.duckdbMapperManagerComponent);
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed(@SuppressWarnings("unused") ContextClosedEvent event) {
        log.info("[Spring Closed] onContextClosed");
        if (jobClient == null) {
            return;
        }

        try {
            // jobClient.cancel().get();
            // 3. 第一步：查询作业当前状态（异步查询，避免阻塞）
            CompletableFuture<JobStatus> jobStatusFuture = jobClient.getJobStatus();
            // 获取状态（设置超时时间，避免无限等待，可根据业务调整）
            JobStatus currentJobStatus = jobStatusFuture.get(10, TimeUnit.SECONDS);

            // 4. 第二步：判断作业是否处于「可取消」的有效状态
            boolean isCancelable = isJobCancelable(currentJobStatus);
            if (isCancelable) {
                System.out.printf("作业当前状态：%s，符合取消条件，开始优雅取消...%n", currentJobStatus);
                // 5. 第三步：执行优雅取消（get() 阻塞等待取消完成，可异步处理）
                jobClient.cancel().get();
                System.out.println("作业取消成功！");
            } else {
                System.out.printf("作业当前状态：%s，无需执行取消操作（已完成/已取消/失败）%n", currentJobStatus);
            }
        } catch (Exception e) {
            log.warn("jobClient cancel failed.", e);
        }
        jobClient = null;
    }

    private static boolean isJobCancelable(JobStatus jobStatus) {
        if (jobStatus == null) {
            return false;
        }
        // 只针对「运行中/已创建/已调度/已暂停」状态执行取消
        return jobStatus == JobStatus.RUNNING        // 作业正常运行（核心取消场景）
                || jobStatus == JobStatus.CREATED     // 作业已创建，未开始执行
                || jobStatus == JobStatus.INITIALIZING; // 作业正在初始化，等待JobManager就绪
    }

    @NonNull
    private static JobClient createMySQLSyncDuckdb(MultipleParameterTool params, DuckdbMapperManagerComponent duckdbMapperManagerComponent) throws Exception {
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.MYSQL_CONF));
        Map<String, String> mysqlMap = getConfigMap(params, DatabaseSyncConfig.MYSQL_CONF);
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        DuckdbDatabaseSync databaseSync = new DuckdbMysqlDatabaseSync(duckdbMapperManagerComponent);
       return syncDuckdb(params, databaseSync, mysqlConfig, SourceConnector.MYSQL);
    }


    @NonNull
    private static JobClient syncDuckdb(
            MultipleParameterTool params,
            DuckdbDatabaseSync databaseSync,
            Configuration config,
            SourceConnector sourceConnector)
            throws Exception {
        String jobName = params.get(DatabaseSyncConfig.JOB_NAME);
        String database = params.get(DatabaseSyncConfig.DATABASE);
        String tablePrefix = params.get(DatabaseSyncConfig.TABLE_PREFIX);
        String tableSuffix = params.get(DatabaseSyncConfig.TABLE_SUFFIX);
        String includingTables = params.get(DatabaseSyncConfig.INCLUDING_TABLES);
        String excludingTables = params.get(DatabaseSyncConfig.EXCLUDING_TABLES);
        String multiToOneOrigin = params.get(DatabaseSyncConfig.MULTI_TO_ONE_ORIGIN);
        String multiToOneTarget = params.get(DatabaseSyncConfig.MULTI_TO_ONE_TARGET);
        String schemaChangeMode = params.get(DatabaseSyncConfig.SCHEMA_CHANGE_MODE);
        boolean createTableOnly = params.has(DatabaseSyncConfig.CREATE_TABLE_ONLY);
        boolean ignoreDefaultValue = params.has(DatabaseSyncConfig.IGNORE_DEFAULT_VALUE);
        boolean ignoreIncompatible = params.has(DatabaseSyncConfig.IGNORE_INCOMPATIBLE);
        boolean singleSink = params.has(DatabaseSyncConfig.SINGLE_SINK);

        Preconditions.checkArgument(params.has(DatabaseSyncConfig.SINK_CONF));
        Map<String, String> sinkMap = getConfigMap(params, DatabaseSyncConfig.SINK_CONF);
        DorisTableConfig tableConfig =
                new DorisTableConfig(getConfigMap(params, DatabaseSyncConfig.TABLE_CONF));
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = null;
        if (true) {
            // suyh - 本地测试 使用 WebUI
            Configuration configuration = new Configuration();
            configuration.set(RestOptions.BIND_PORT, "8082");
            // 启用/禁用算子链
            configuration.setString("pipeline.operator-chaining.enabled", "false");
            configuration.setString("parallelism.default", "1");
            configuration.setString("execution.checkpointing.min-pause", "10000");
            configuration.setInteger("state.checkpoints.num-retained", 2);

            if (true) {
                // 从checkpoint 启动
                String checkpointPath = "file:///E:\\tmp\\cem-cdc\\checkpoints\\6eb5c92dcb5914a929054ac82dce6dd7\\chk-32\\_metadata";
                configuration.setString("execution.savepoint.path", checkpointPath);
            }
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

            // 1. 开启周期性Checkpoint，间隔60秒
            env.enableCheckpointing(60_000);
            // 2. 设置状态后端：Flink 1.18 推荐使用 HashMapStateBackend（内存管理）或 EmbeddedRocksDBStateBackend
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
            // 3. 设置 Checkpoint 存储路径（文件系统，实现状态的文件持久化，即「文件型状态后端」核心）
            // 本地文件系统：Windows 格式 file:///E:\\tmp\\cem-cdc\\checkpoints，Linux/Mac 格式 file:///tmp/flink-checkpoints
            // 分布式文件系统（生产环境）：hdfs://xxx:9000/flink/checkpoints 或 oss://xxx/flink/checkpoints
            env.getCheckpointConfig().setCheckpointStorage("file:///E:\\tmp\\cem-cdc\\checkpoints");

            // 4. 高级配置（保障文件型Checkpoint的可靠性）
            CheckpointConfig ckConfig = env.getCheckpointConfig();
            // 确保 Checkpointing 模式为 EXACTLY_ONCE（默认即是）
            // ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            ckConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE); // 至少一次
            // 任务取消后保留文件型Checkpoint数据（核心：文件存储的Checkpoint不会被删除，可用于后续恢复）
            ckConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            // 设置 Checkpoint 超时时间（10分钟）
            ckConfig.setCheckpointTimeout(600_000);

            // 禁止失败重试：一旦出错，立即停止任务（本地调试用）
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConfig)
                .setTableConfig(tableConfig)
                .setCreateTableOnly(createTableOnly)
                .setSingleSink(singleSink)
                .setIgnoreIncompatible(ignoreIncompatible)
                .setSchemaChangeMode(schemaChangeMode)
                .create();

        boolean needExecute = databaseSync.build();
        if (!needExecute) {
            // create table only
            return null;
        }
        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName =
                    String.format(
                            "%s-Doris Sync Database: %s",
                            sourceConnector.getConnectorName(),
                            config.getString(
                                    DatabaseSyncConfig.DATABASE_NAME, DatabaseSyncConfig.DB));
        }
        return env.executeAsync(jobName);
    }

    @VisibleForTesting
    public static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            System.out.println(
                    "Can not find key ["
                            + key
                            + "] from args: "
                            + params.toMap().toString()
                            + ".\n");
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0].trim(), kv[1].trim());
                continue;
            } else if (kv.length == 1 && EMPTY_KEYS.contains(kv[0])) {
                map.put(kv[0].trim(), "");
                continue;
            }

            System.out.println("Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}
