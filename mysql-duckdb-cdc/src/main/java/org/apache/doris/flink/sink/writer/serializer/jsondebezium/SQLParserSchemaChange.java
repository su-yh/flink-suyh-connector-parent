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

package org.apache.doris.flink.sink.writer.serializer.jsondebezium;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.sink.schema.SQLParserSchemaManager;
import org.apache.doris.flink.sink.writer.EventType;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/** Schema changes are made by parsing upstream DDL statements. */
public class SQLParserSchemaChange extends JsonDebeziumSchemaChange {
    private static final Logger LOG = LoggerFactory.getLogger(SQLParserSchemaChange.class);
    private static final long serialVersionUID = 6482855986545948737L;
    private final SQLParserSchemaManager sqlParserSchemaManager;
    private final String duckdbTableName;

    public SQLParserSchemaChange(String duckdbTableName) {
        // this.changeContext = changeContext;
        // this.dorisOptions = changeContext.getDorisOptions();
        // this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.sqlParserSchemaManager = new SQLParserSchemaManager();
        // this.tableMapping = changeContext.getTableMapping();
        // this.objectMapper = changeContext.getObjectMapper();
        // this.targetDatabase = changeContext.getTargetDatabase();
        // this.dorisTableConfig = changeContext.getDorisTableConf();
        // this.targetTablePrefix =
        //         changeContext.getTargetTablePrefix() == null
        //                 ? ""
        //                 : changeContext.getTargetTablePrefix();
        // this.targetTableSuffix =
        //         changeContext.getTargetTableSuffix() == null
        //                 ? ""
        //                 : changeContext.getTargetTableSuffix();
        // this.tableNameConverter = changeContext.getTableNameConverter();
        this.duckdbTableName = String.format("\"main\".\"%s\"", duckdbTableName);
    }

    @Override
    public void init(JsonNode recordRoot, String dorisTableName) {
        // do nothing
    }

    @Override
    public boolean schemaChange(JsonNode historyRecord) {
        System.out.println("suyh - schemaChange: " + historyRecord);   // suyh
        boolean status = false;
        try {
            // if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
            //     return false;
            // }

            EventType eventType = extractEventType(historyRecord);
            if (eventType == null) {
                LOG.warn("Failed to parse eventType. historyRecord={}", historyRecord);
                return false;
            }

            if (eventType.equals(EventType.CREATE)) {
                // String dorisTable = getCreateTableIdentifier(recordRoot);
                // TableSchema tableSchema = tryParseCreateTableStatement(recordRoot, dorisTable);
                // status = schemaChangeManager.createTable(tableSchema);
                // if (status) {
                //     String cdcTbl = getCdcTableIdentifier(recordRoot);
                //     String dorisTbl = getCreateTableIdentifier(recordRoot);
                //     changeContext.getTableMapping().put(cdcTbl, dorisTbl);
                //     this.tableMapping = changeContext.getTableMapping();
                //     LOG.info(
                //             "create table ddl status: {}, add tableMapping {},{}",
                //             status,
                //             cdcTbl,
                //             dorisTbl);
                // }
            } else if (eventType.equals(EventType.ALTER)) {
                // suyh - 这里就是生成doris 数据库的唯一表，就是映射的表。即： dbName.tableName
                // Tuple2<String, String> dorisTableTuple = getDorisTableTuple(recordRoot);
                // if (dorisTableTuple == null) {
                //     LOG.warn("Failed to get doris table tuple. record={}", recordRoot);
                //     return false;
                // }
                List<String> ddlList = tryParseAlterDDLs(historyRecord);
                status = executeAlterDDLs(ddlList, status);
            }
        } catch (Exception ex) {
            LOG.warn("schema change error : ", ex);
        }
        return status;
    }

    // public TableSchema tryParseCreateTableStatement(JsonNode record, String dorisTable)
    //         throws IOException {
    //     JsonNode historyRecord = extractHistoryRecord(record);
    //     String ddl = extractJsonNode(historyRecord, "ddl");
    //     extractSourceConnector(record);
    //     return sqlParserSchemaManager.parseCreateTableStatement(
    //             sourceConnector, ddl, dorisTable, dorisTableConfig);
    // }

    public List<String> tryParseAlterDDLs(JsonNode historyRecord) throws IOException {
        // String dorisTable =
        //         JsonDebeziumChangeUtils.getDorisTableIdentifier(record, dorisOptions, tableMapping);
        // JsonNode historyRecord = extractHistoryRecord(record);
        String ddl = extractJsonNode(historyRecord, "ddl");
        ddl = ddl.replace("`", "\"");
        // extractSourceConnector(record);  // suyh - 不需要了，这里是提取源数据库是什么数据库，我们这里只处理mysql -> duckdb
        // String duckdbTableName = "db.prefix_tb_user";
        return sqlParserSchemaManager.parseAlterDDLs(SourceConnector.MYSQL, ddl, duckdbTableName);
    }
}
