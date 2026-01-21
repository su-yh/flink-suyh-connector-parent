package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Slf4j
public class TruncateTable extends AbstractMethod {
    private static final long serialVersionUID = -5992659898495039832L;
    // SqlMethod
    private static final String METHOD = "truncateTable";

    public TruncateTable() {
        super(METHOD);
    }

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        String tableName = tableInfo.getTableName();

        String sqlScript = buildTruncateTableSqlScript(tableName);
        log.debug("sqlScript: {}", sqlScript);
        Class<?> parameterType = null;  // 参数类型
        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sqlScript, parameterType);
        return this.addInsertMappedStatement(
                mapperClass, modelClass, super.methodName, sqlSource,
                NoKeyGenerator.INSTANCE, null, null);
    }

    private static String buildTruncateTableSqlScript(String tableName) {
        return "<script>TRUNCATE TABLE " + tableName + "</script>";
    }
}
