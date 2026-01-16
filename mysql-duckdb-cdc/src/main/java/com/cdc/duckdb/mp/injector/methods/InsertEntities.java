package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.toolkit.sql.SqlScriptUtils;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class InsertEntities extends AbstractMethod {
    // SqlMethod
    private static final String METHOD = "insertEntities";
    private static final String DESC = "批量插入";
    private static final String SQL = "<script>\nINSERT INTO %s %s VALUES %s\n</script>";

    public InsertEntities() {
        super(METHOD);
    }

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        tableInfo.getKeyColumn();
        tableInfo.getKeyProperty();

        List<String> columns = new ArrayList<>();
        List<String> properties = new ArrayList<>();

        columns.add(tableInfo.getKeyColumn());
        // item 是 foreach 的循环变量
        properties.add("#{item." + tableInfo.getKeyProperty() + "}");

        List<TableFieldInfo> insertFields = tableInfo.getFieldList();   // 这个方法直接不会包含主键ID 列
        for (TableFieldInfo fieldInfo : insertFields) {
            columns.add(fieldInfo.getColumn());
            // item 是 foreach 的循环变量
            properties.add("#{item." + fieldInfo.getProperty() + "}");
        }

        String columnSql = LEFT_BRACKET + String.join(COMMA, columns) + RIGHT_BRACKET;
        String singleValueScript = LEFT_BRACKET + String.join(COMMA, properties) + RIGHT_BRACKET;

        String multiValueScript = SqlScriptUtils.convertForeach(singleValueScript, BaseMapperDuckdb.ENTITIES, null, "item", COMMA);

        String sql = String.format(SQL, tableInfo.getTableName(), columnSql, multiValueScript);

        log.debug("生成的批量插入 SQL：" + sql);

        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sql, List.class);
        return this.addInsertMappedStatement(
                mapperClass, modelClass, super.methodName, sqlSource,
                NoKeyGenerator.INSTANCE, null, null
        );
    }
}
