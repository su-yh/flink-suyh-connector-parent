package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.cdc.duckdb.mp.ann.TbColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

import java.util.ArrayList;
import java.util.List;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Slf4j
public class CreateTableIfNotExists extends AbstractMethod {
    // SqlMethod
    private static final String METHOD = "createTableIfNotExists";
    private static final String DESC = "当表不存在时创建表";

    public CreateTableIfNotExists() {
        super(METHOD);
    }

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        String tableName = tableInfo.getTableName();

        List<TbColumn> columns = new ArrayList<>();
        InjectorUtils.extractDuckdbColumnType(columns, null, modelClass);

        String sqlScript = buildCreateTableSqlScript(tableName, columns);
        log.debug("sqlScript: {}", sqlScript);
        Class<?> parameterType = null;  // 参数类型
        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sqlScript, parameterType);
        return this.addInsertMappedStatement(
                mapperClass, modelClass, super.methodName, sqlSource,
                NoKeyGenerator.INSTANCE, null, null);
    }

    private static String buildCreateTableSqlScript(String tableName, List<TbColumn> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("<script>CREATE TABLE IF NOT EXISTS ");
        sb.append(tableName);
        sb.append("(");

        List<String> columDefine = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        for (TbColumn column : columns) {
            columDefine.add('"' + column.value() + '"' + " " + column.type());

            if (column.primaryKey()) {
                primaryKeys.add(column.value());
            }
        }

        sb.append(String.join(",", columDefine));

        if (!primaryKeys.isEmpty()) {
            sb.append(", PRIMARY KEY(");
            sb.append(String.join(",", primaryKeys));
            sb.append(")");
        }

        sb.append(")");
        sb.append("</script>");

        return sb.toString();
    }
}
