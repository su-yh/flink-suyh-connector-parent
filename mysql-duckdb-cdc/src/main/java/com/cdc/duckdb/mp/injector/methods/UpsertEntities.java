package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.cdc.duckdb.mp.ann.TbColumn;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Slf4j
public class UpsertEntities extends AbstractMethod {
    // SqlMethod
    private static final String METHOD = "upsertEntities";
    private static final String DESC = "当表不存在时创建表";
    // 官方文档上的示例：
    // CREATE TABLE tbl (i INTEGER PRIMARY KEY, j INTEGER);
    // INSERT INTO tbl VALUES (1, 42);
    // INSERT INTO tbl VALUES (1, 52), (1, 62) ON CONFLICT DO UPDATE SET j = EXCLUDED.j;
    private static final String SQL = "<script>\nINSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s\n</script>";

    public UpsertEntities() {
        super(METHOD);
    }

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        String tableName = tableInfo.getTableName();

        List<TbColumn> columns = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        InjectorUtils.extractDuckdbColumnType(columns, fieldNames, modelClass);

        String sqlScript = buildUpsertSqlScript(tableName, columns, fieldNames);
        log.debug("upsertEntities, sql script: \n{}", sqlScript);

        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sqlScript, Collection.class);
        return this.addInsertMappedStatement(
                mapperClass, modelClass, super.methodName, sqlSource,
                NoKeyGenerator.INSTANCE, null, null);
    }

    private String buildUpsertSqlScript(String tableName, List<TbColumn> columns, List<String> fieldNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("<script>\n");
        sb.append("INSERT INTO ");
        sb.append(tableName);
        String columnNames = columns.stream().map(TbColumn::value).collect(Collectors.joining(",", "(", ")"));
        sb.append(columnNames);
        sb.append("\n");
        sb.append("VALUES\n");
        sb.append("<foreach collection='" + BaseMapperDuckdb.ENTITIES + "' item='entity' separator=','>");
        String entityFields = fieldNames.stream().map(name -> "#{entity." + name + "}").collect(Collectors.joining(",", "(", ")"));
        sb.append(entityFields);
        sb.append("</foreach>");
        sb.append("\n");
        sb.append("ON CONFLICT\n");
        sb.append("DO UPDATE SET ");
        String excluded = columns.stream().filter(tbColumn -> !tbColumn.primaryKey())
                .map(tbColumn -> tbColumn.value() + "=EXCLUDED." + tbColumn.value()).collect(Collectors.joining(","));
        sb.append(excluded);
        sb.append("\n");
        sb.append("</script>");

        return sb.toString();
    }
}
