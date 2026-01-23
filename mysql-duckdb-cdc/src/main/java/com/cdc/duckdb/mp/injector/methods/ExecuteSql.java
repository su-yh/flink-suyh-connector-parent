package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Slf4j
public class ExecuteSql extends AbstractMethod {
    private static final long serialVersionUID = 4402331192104897355L;
    // SqlMethod
    private static final String METHOD = "executeSql";

    public ExecuteSql() {
        super(METHOD);
    }

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        String sqlScript = buildTruncateTableSqlScript();
        log.debug("sqlScript: {}", sqlScript);
        Class<?> parameterType = null;  // 参数类型
        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sqlScript, parameterType);
        return this.addInsertMappedStatement(
                mapperClass, modelClass, super.methodName, sqlSource,
                NoKeyGenerator.INSTANCE, null, null);
    }

    private static String buildTruncateTableSqlScript() {
        return "<script>${" + BaseMapperDuckdb.SQL_TEXT_KEY + "}</script>";
    }
}
