package com.cdc.duckdb.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * @author suyh
 * @since 2026-01-12
 */
@Component
@Slf4j
public class CdcComponent implements InitializingBean {
    public static String[] args = null;


    @Override
    public void afterPropertiesSet() throws Exception {
        // String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        // MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        // createMySQLSyncDuckdb(params);
    }

    // private static void createMySQLSyncDuckdb(MultipleParameterTool params) throws Exception {
    //     Preconditions.checkArgument(params.has(DatabaseSyncConfig.MYSQL_CONF));
    //     Map<String, String> mysqlMap = getConfigMap(params, DatabaseSyncConfig.MYSQL_CONF);
    //     Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
    //     DuckdbDatabaseSync databaseSync = new DuckdbMysqlDatabaseSync();
    //     syncDuckdb(params, databaseSync, mysqlConfig, SourceConnector.MYSQL);
    // }
    //
    //
    // private static void syncDuckdb(
    //         MultipleParameterTool params,
    //         DuckdbDatabaseSync databaseSync,
    //         Configuration config,
    //         SourceConnector sourceConnector)
    //         throws Exception {
    //     String jobName = params.get(DatabaseSyncConfig.JOB_NAME);
    //     String database = params.get(DatabaseSyncConfig.DATABASE);
    //     String tablePrefix = params.get(DatabaseSyncConfig.TABLE_PREFIX);
    //     String tableSuffix = params.get(DatabaseSyncConfig.TABLE_SUFFIX);
    //     String includingTables = params.get(DatabaseSyncConfig.INCLUDING_TABLES);
    //     String excludingTables = params.get(DatabaseSyncConfig.EXCLUDING_TABLES);
    //     String multiToOneOrigin = params.get(DatabaseSyncConfig.MULTI_TO_ONE_ORIGIN);
    //     String multiToOneTarget = params.get(DatabaseSyncConfig.MULTI_TO_ONE_TARGET);
    //     String schemaChangeMode = params.get(DatabaseSyncConfig.SCHEMA_CHANGE_MODE);
    //     boolean createTableOnly = params.has(DatabaseSyncConfig.CREATE_TABLE_ONLY);
    //     boolean ignoreDefaultValue = params.has(DatabaseSyncConfig.IGNORE_DEFAULT_VALUE);
    //     boolean ignoreIncompatible = params.has(DatabaseSyncConfig.IGNORE_INCOMPATIBLE);
    //     boolean singleSink = params.has(DatabaseSyncConfig.SINGLE_SINK);
    //
    //     Preconditions.checkArgument(params.has(DatabaseSyncConfig.SINK_CONF));
    //     Map<String, String> sinkMap = getConfigMap(params, DatabaseSyncConfig.SINK_CONF);
    //     DorisTableConfig tableConfig =
    //             new DorisTableConfig(getConfigMap(params, DatabaseSyncConfig.TABLE_CONF));
    //     Configuration sinkConfig = Configuration.fromMap(sinkMap);
    //
    //     // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //     StreamExecutionEnvironment env = null;
    //     if (true) {
    //         // suyh - 本地测试 使用 WebUI
    //         Configuration configuration = new Configuration();
    //         configuration.set(RestOptions.BIND_PORT, "8082");
    //         // 启用/禁用算子链
    //         configuration.setString("pipeline.operator-chaining.enabled", "false");
    //         configuration.setString("parallelism.default", "1");
    //         // configuration.setInteger("state.checkpoints.num-retained", 2);
    //
    //         // 从checkpoint 启动
    //         // String checkpointPath = "file:///E:\\tmp\\checkpoints\\b4c76e988a362b078a914215ea4d88f6\\chk-222\\_metadata";
    //         // configuration.setString("execution.savepoint.path", checkpointPath);
    //         env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    //
    //         // 1. 开启周期性Checkpoint，间隔30秒（本地调试可缩短，如5秒=5000ms）
    //         env.enableCheckpointing(60000);
    //         // 2. 设置状态后端：Flink 1.18 推荐使用 HashMapStateBackend（内存管理）或 EmbeddedRocksDBStateBackend
    //         env.setStateBackend(new HashMapStateBackend());
    //         // 3. 设置 Checkpoint 存储路径（存储到本地文件系统）
    //         // 注意：Windows 环境下路径示例 "file:///D:/flink-checkpoints"
    //         //      Linux/Mac 环境下路径示例 "file:///tmp/flink-checkpoints"
    //         env.getCheckpointConfig().setCheckpointStorage("file:///E:\\tmp\\checkpoints");
    //
    //         // 4. (可选) 高级配置
    //         CheckpointConfig ckConfig = env.getCheckpointConfig();
    //         // 确保 Checkpointing 模式为 EXACTLY_ONCE（默认即是）
    //         ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    //         // 任务取消后保留 Checkpoint 数据（方便调试查看文件）
    //         ckConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    //         // 设置 Checkpoint 超时时间
    //         ckConfig.setCheckpointTimeout(60000);
    //
    //         // 禁止失败重试：一旦出错，立即停止任务
    //         env.setRestartStrategy(RestartStrategies.noRestart());
    //     }
    //     databaseSync
    //             .setEnv(env)
    //             .setDatabase(database)
    //             .setConfig(config)
    //             .setTablePrefix(tablePrefix)
    //             .setTableSuffix(tableSuffix)
    //             .setIncludingTables(includingTables)
    //             .setExcludingTables(excludingTables)
    //             .setMultiToOneOrigin(multiToOneOrigin)
    //             .setMultiToOneTarget(multiToOneTarget)
    //             .setIgnoreDefaultValue(ignoreDefaultValue)
    //             .setSinkConfig(sinkConfig)
    //             .setTableConfig(tableConfig)
    //             .setCreateTableOnly(createTableOnly)
    //             .setSingleSink(singleSink)
    //             .setIgnoreIncompatible(ignoreIncompatible)
    //             .setSchemaChangeMode(schemaChangeMode)
    //             .create();
    //
    //     boolean needExecute = databaseSync.build();
    //     if (!needExecute) {
    //         // create table only
    //         return;
    //     }
    //     if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
    //         jobName =
    //                 String.format(
    //                         "%s-Doris Sync Database: %s",
    //                         sourceConnector.getConnectorName(),
    //                         config.getString(
    //                                 DatabaseSyncConfig.DATABASE_NAME, DatabaseSyncConfig.DB));
    //     }
    //     env.execute(jobName);
    // }
}
