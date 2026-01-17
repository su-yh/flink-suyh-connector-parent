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

package org.apache.duckdb.sink;

import com.cdc.duckdb.component.DuckdbMapperManagerComponent;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.debezium.data.Envelope;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.sink.schema.SchemaChangeMode;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;

public abstract class DuckdbDatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DuckdbDatabaseSync.class);
    private static final String TABLE_NAME_OPTIONS = "table-name";

    protected Configuration config;

    // sink 的目标要数据库名
    protected String database;

    protected TableNameConverter converter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<Pattern, String> multiToOneRulesPattern;
    protected DorisTableConfig dorisTableConfig;
    protected Configuration sinkConfig;
    protected boolean ignoreDefaultValue;
    protected boolean ignoreIncompatible;

    public StreamExecutionEnvironment env;
    private boolean createTableOnly = false;
    private boolean newSchemaChange = true;
    private String schemaChangeMode;
    protected String includingTables;
    protected String excludingTables;
    protected String multiToOneOrigin;
    protected String multiToOneTarget;
    protected String tablePrefix;
    protected String tableSuffix;
    protected boolean singleSink;
    protected final DuckdbMapperManagerComponent duckdbMapperManagerComponent;

    public abstract void registerDriver() throws SQLException;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);

    /** Get the prefix of a specific tableList, for example, mysql is database, oracle is schema. */
    public abstract String getTableListPrefix();

    protected DuckdbDatabaseSync(DuckdbMapperManagerComponent duckdbMapperManagerComponent) throws SQLException {
        this.duckdbMapperManagerComponent = duckdbMapperManagerComponent;
        registerDriver();
    }

    public void create() {
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.multiToOneRulesPattern = multiToOneRulesParser(multiToOneOrigin, multiToOneTarget);
        this.converter = new TableNameConverter(tablePrefix, tableSuffix, multiToOneRulesPattern);
    }

    public boolean build() throws Exception {
        DorisConnectionOptions options = getDorisConnectionOptions();
        DorisSystem dorisSystem = new DorisSystem(options);

        List<SourceSchema> schemaList = getSchemaList();
        Preconditions.checkState(
                !schemaList.isEmpty(),
                "No tables to be synchronized. Please make sure whether the tables that need to be synchronized exist in the corresponding database or schema.");

        if (!StringUtils.isNullOrWhitespaceOnly(database)
                && !dorisSystem.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            dorisSystem.createDatabase(database);
        }
        List<String> syncTables = new ArrayList<>();

        Set<String> targetDbSet = new HashSet<>();
        for (SourceSchema schema : schemaList) {
            List<String> primaryKeys = schema.getPrimaryKeys();
            int size = primaryKeys == null ? 0 : primaryKeys.size();
            if (size != 1) {
                LOG.warn("Unsupported. source db table name: {}, primary key size: {}. primary key size must 1.",
                        schema.getTableName(), size);
                continue;
            }

            duckdbMapperManagerComponent.registerMapperBean(schema);
            BaseMapperDuckdb<?> baseMapperBean = duckdbMapperManagerComponent.getMapperBean(schema.getTableName());
            baseMapperBean.createTableIfNotExists();

            syncTables.add(schema.getTableName());
            String targetDb = database;
            // Synchronize multiple databases using the src database name
            if (StringUtils.isNullOrWhitespaceOnly(targetDb)) {
                targetDb = schema.getDatabaseName();
                targetDbSet.add(targetDb);
            }
            if (StringUtils.isNullOrWhitespaceOnly(database)
                    && !dorisSystem.databaseExists(targetDb)) {
                LOG.info("database {} not exist, created", targetDb);
                dorisSystem.createDatabase(targetDb);
            }
        }
        if (createTableOnly) {
            System.out.println("Create table finished.");
            return false;
        }
        config.setString(TABLE_NAME_OPTIONS, getSyncTableList(syncTables));
        DataStreamSource<String> streamSource = buildCdcSource(env);
        SingleOutputStreamOperator<RecordDto> recordDtoDataSource = streamSource.map(new MapFunction<String, RecordDto>() {
            @Override
            public RecordDto map(String value) throws Exception {
                if (StringUtils.isNullOrWhitespaceOnly(value)) {
                    return null;
                }

                JsonNode recordRoot = JsonUtils.deserialize(value, JsonNode.class);
                if (recordRoot == null) {
                    return null;
                }

                RecordDto dto = new RecordDto();

                {
                    JsonNode opNode = recordRoot.get(Envelope.FieldName.OPERATION);
                    if (opNode != null && !(opNode instanceof NullNode)) {
                        dto.setOperation(opNode.asText());
                    }
                }
                {
                    JsonNode tsNode = recordRoot.get(Envelope.FieldName.TIMESTAMP);
                    if (tsNode != null && !(tsNode instanceof NullNode)) {
                        dto.setTimestamp(tsNode.asLong());
                    }
                }
                {
                    JsonNode beforeNode = recordRoot.get(Envelope.FieldName.BEFORE);
                    if (beforeNode != null && !(beforeNode instanceof NullNode)) {
                        dto.setBeforeJson(beforeNode.toString());
                    }
                }
                {
                    JsonNode afterNode = recordRoot.get(Envelope.FieldName.AFTER);
                    if (afterNode != null && !(afterNode instanceof NullNode)) {
                        dto.setAfterJson(afterNode.toString());
                    }
                }
                {
                    JsonNode sourceNode = recordRoot.get(Envelope.FieldName.SOURCE);
                    if (sourceNode != null && !(sourceNode instanceof NullNode)) {
                        String sourceJson = sourceNode.toString();
                        if (!StringUtils.isNullOrWhitespaceOnly(sourceJson)) {
                            CdcSourceMetaDto source = JsonUtils.deserialize(sourceJson, CdcSourceMetaDto.class);
                            dto.setSource(source);
                        }
                    }
                }
                {
                    JsonNode transactionNode = recordRoot.get(Envelope.FieldName.TRANSACTION);
                    if (transactionNode != null && !(transactionNode instanceof NullNode)) {
                        dto.setTransaction(transactionNode.toString());
                    }
                }

                return dto;
            }
        });

        SingleOutputStreamOperator<RecordDto> filterDataSource = recordDtoDataSource.filter(Objects::nonNull);
        filterDataSource.sinkTo(new DuckDBSink());
        return true;
    }

    private DorisConnectionOptions getDorisConnectionOptions() {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String benodes = sinkConfig.getString(DorisConfigOptions.BENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.getString(DorisConfigOptions.JDBC_URL);
        Preconditions.checkNotNull(fenodes, "fenodes is empty in sink-conf");
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");
        DorisConnectionOptions.DorisConnectionOptionsBuilder builder =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes(fenodes)
                        .withBenodes(benodes)
                        .withUsername(user)
                        .withPassword(passwd)
                        .withJdbcUrl(jdbcUrl);
        return builder.build();
    }

    /** Filter table that need to be synchronized. */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }

    protected String getSyncTableList(List<String> syncTables) {
        if (!singleSink) {
            if (false) { // if (this instanceof OracleDatabaseSync) {
                return "";
            } else {
                return String.format(
                        "(%s)\\.(%s)", getTableListPrefix(), String.join("|", syncTables));
            }
        } else {
            // includingTablePattern and ^excludingPattern
            if (includingTables == null) {
                includingTables = ".*";
            }
            String includingPattern =
                    String.format("(%s)\\.(%s)", getTableListPrefix(), includingTables);
            if (StringUtils.isNullOrWhitespaceOnly(excludingTables)) {
                return includingPattern;
            } else {
                String excludingPattern =
                        String.format("?!(%s\\.(%s))$", getTableListPrefix(), excludingTables);
                return String.format("(%s)(%s)", excludingPattern, includingPattern);
            }
        }
    }

    /** Filter table that many tables merge to one. */
    protected HashMap<Pattern, String> multiToOneRulesParser(
            String multiToOneOrigin, String multiToOneTarget) {
        if (StringUtils.isNullOrWhitespaceOnly(multiToOneOrigin)
                || StringUtils.isNullOrWhitespaceOnly(multiToOneTarget)) {
            return null;
        }
        HashMap<Pattern, String> multiToOneRulesPattern = new HashMap<>();
        String[] origins = multiToOneOrigin.split("\\|");
        String[] targets = multiToOneTarget.split("\\|");
        if (origins.length != targets.length) {
            System.out.println(
                    "param error : multi to one params length are not equal,please check your params.");
            System.exit(1);
        }
        try {
            for (int i = 0; i < origins.length; i++) {
                multiToOneRulesPattern.put(Pattern.compile(origins[i]), targets[i]);
            }
        } catch (Exception e) {
            System.out.println("param error : Your regular expression is incorrect,please check.");
            System.exit(1);
        }
        return multiToOneRulesPattern;
    }

    /**
     * Get table buckets Map.
     *
     * @param tableBuckets the string of tableBuckets, eg:student:10,student_info:20,student.*:30
     * @return The table name and buckets map. The key is table name, the value is buckets.
     */
    @Deprecated
    public static Map<String, Integer> getTableBuckets(String tableBuckets) {
        Map<String, Integer> tableBucketsMap = new LinkedHashMap<>();
        String[] tableBucketsArray = tableBuckets.split(",");
        for (String tableBucket : tableBucketsArray) {
            String[] tableBucketArray = tableBucket.split(":");
            tableBucketsMap.put(
                    tableBucketArray[0].trim(), Integer.parseInt(tableBucketArray[1].trim()));
        }
        return tableBucketsMap;
    }

    /**
     * Set table schema buckets.
     *
     * @param tableBucketsMap The table name and buckets map. The key is table name, the value is
     *     buckets.
     * @param dorisSchema @{TableSchema}
     * @param dorisTable the table name need to set buckets
     * @param tableHasSet The buckets table is set
     */
    @Deprecated
    public void setTableSchemaBuckets(
            Map<String, Integer> tableBucketsMap,
            TableSchema dorisSchema,
            String dorisTable,
            Set<String> tableHasSet) {

        if (tableBucketsMap != null) {
            // Firstly, if the table name is in the table-buckets map, set the buckets of the table.
            if (tableBucketsMap.containsKey(dorisTable)) {
                dorisSchema.setTableBuckets(tableBucketsMap.get(dorisTable));
                tableHasSet.add(dorisTable);
                return;
            }
            // Secondly, iterate over the map to find a corresponding regular expression match,
            for (Map.Entry<String, Integer> entry : tableBucketsMap.entrySet()) {
                if (tableHasSet.contains(entry.getKey())) {
                    continue;
                }

                Pattern pattern = Pattern.compile(entry.getKey());
                if (pattern.matcher(dorisTable).matches()) {
                    dorisSchema.setTableBuckets(entry.getValue());
                    tableHasSet.add(dorisTable);
                    return;
                }
            }
        }
    }

    protected Properties getJdbcProperties() {
        Properties jdbcProps = new Properties();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                jdbcProps.put(key.substring(PROPERTIES_PREFIX.length()), value);
            }
        }
        return jdbcProps;
    }

    protected String getJdbcUrlTemplate(String initialJdbcUrl, Properties jdbcProperties) {
        StringBuilder jdbcUrlBuilder = new StringBuilder(initialJdbcUrl);
        jdbcProperties.forEach(
                (key, value) -> jdbcUrlBuilder.append("&").append(key).append("=").append(value));
        return jdbcUrlBuilder.toString();
    }

    public DuckdbDatabaseSync setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public DuckdbDatabaseSync setConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public DuckdbDatabaseSync setDatabase(String database) {
        this.database = database;
        return this;
    }

    public DuckdbDatabaseSync setIncludingTables(String includingTables) {
        this.includingTables = includingTables;
        return this;
    }

    public DuckdbDatabaseSync setExcludingTables(String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public DuckdbDatabaseSync setMultiToOneOrigin(String multiToOneOrigin) {
        this.multiToOneOrigin = multiToOneOrigin;
        return this;
    }

    public DuckdbDatabaseSync setMultiToOneTarget(String multiToOneTarget) {
        this.multiToOneTarget = multiToOneTarget;
        return this;
    }

    @Deprecated
    public DuckdbDatabaseSync setTableConfig(Map<String, String> tableConfig) {
        if (!CollectionUtil.isNullOrEmpty(tableConfig)) {
            this.dorisTableConfig = new DorisTableConfig(tableConfig);
        }
        return this;
    }

    public DuckdbDatabaseSync setTableConfig(DorisTableConfig tableConfig) {
        this.dorisTableConfig = tableConfig;
        return this;
    }

    public DuckdbDatabaseSync setSinkConfig(Configuration sinkConfig) {
        this.sinkConfig = sinkConfig;
        return this;
    }

    public DuckdbDatabaseSync setIgnoreDefaultValue(boolean ignoreDefaultValue) {
        this.ignoreDefaultValue = ignoreDefaultValue;
        return this;
    }

    public DuckdbDatabaseSync setCreateTableOnly(boolean createTableOnly) {
        this.createTableOnly = createTableOnly;
        return this;
    }

    public DuckdbDatabaseSync setNewSchemaChange(boolean newSchemaChange) {
        this.newSchemaChange = newSchemaChange;
        return this;
    }

    public DuckdbDatabaseSync setSchemaChangeMode(String schemaChangeMode) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(schemaChangeMode)) {
            this.schemaChangeMode = SchemaChangeMode.DEBEZIUM_STRUCTURE.getName();
            return this;
        }
        this.schemaChangeMode = schemaChangeMode.trim();
        return this;
    }

    public DuckdbDatabaseSync setSingleSink(boolean singleSink) {
        this.singleSink = singleSink;
        return this;
    }

    public DuckdbDatabaseSync setIgnoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    public DuckdbDatabaseSync setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public DuckdbDatabaseSync setTableSuffix(String tableSuffix) {
        this.tableSuffix = tableSuffix;
        return this;
    }
}
