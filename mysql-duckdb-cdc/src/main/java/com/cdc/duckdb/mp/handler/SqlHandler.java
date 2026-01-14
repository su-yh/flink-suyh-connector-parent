package com.cdc.duckdb.mp.handler;

import com.baomidou.dynamic.datasource.toolkit.DynamicDataSourceContextHolder;
import com.cdc.duckdb.config.datasource.DataSourceNames;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

@Intercepts({
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class})
})
@Slf4j
public class SqlHandler implements Interceptor {
    public static final ThreadLocal<List<String>> AUDIT_SQL_LIST = new ThreadLocal<>();
    public static boolean LOG_STATUS = true;
    // suyh - 暂时在测试环境使用，没有问题之后再开放到线上。分页查询时的SQL 明细。
    public static boolean PLUS_LOG_ENABLED = false;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object proceed = invocation.proceed();
        try {
            doSqlLogRecord(invocation);
        } catch (Exception exception) {
            log.error("SqlHandler invoke failed", exception);
        }
        return proceed;
    }

    private void doSqlLogRecord(Invocation invocation) {
        Object[] args = invocation.getArgs();
        MappedStatement mappedStatement = (MappedStatement) args[0];
        try (Connection connection = mappedStatement.getConfiguration().getEnvironment().getDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            BoundSql boundSql = mappedStatement.getBoundSql(args[1]);
            if (PLUS_LOG_ENABLED && invocation.getMethod().getName().equals("query") && args.length == 6 && args[5] != null) {
                boundSql = (BoundSql) args[5];
            }
            Object parameterObject = args[1];

            // Create PreparedStatement object for SQL query
            try (PreparedStatement preparedStatement = connection.prepareStatement(boundSql.getSql())) {
                DefaultParameterHandler defaultParameterHandler = new DefaultParameterHandler(mappedStatement, parameterObject, boundSql);
                defaultParameterHandler.setParameters(preparedStatement);
                String sqlDetail = preparedStatement.toString();
                if (LOG_STATUS && !hasNoSqlLogAnnotation(invocation)) {
                    Logger logger = obtainLogger(mappedStatement);
                    String dataSourceName = DynamicDataSourceContextHolder.peek();
                    logger.info("\nData source: {}({}), URL: {}, sql: \n{}",
                            StringUtils.hasText(dataSourceName) ? dataSourceName : DataSourceNames.CEM_MASTER_MYSQL, metaData.getDatabaseProductName(), extractUri(metaData.getURL()), sqlDetail);
                }

                String name = invocation.getMethod().getName();
                if (name.equalsIgnoreCase("update")) {
                    List<String> sqlList = SqlHandler.AUDIT_SQL_LIST.get();
                    if (sqlList != null) {
                        sqlList.add(sqlDetail);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("SQL execution error", e);
        }
    }

    private Logger obtainLogger(MappedStatement mappedStatement) {
        String mapperInterfaceName = mappedStatement.getId().substring(0, mappedStatement.getId().lastIndexOf("."));
        Logger slf4jLogger = LoggerFactory.getLogger(mapperInterfaceName);
        return slf4jLogger == null ? log : slf4jLogger;
    }

    private boolean hasNoSqlLogAnnotation(Invocation invocation) {
        Object[] args = invocation.getArgs();
        if (args.length == 0 || !(args[0] instanceof MappedStatement)) {
            return false;
        }
        MappedStatement ms = (MappedStatement) args[0];
        String id = ms.getId(); // 例如 com.xxx.UserMapper.findById
        String className = id.substring(0, id.lastIndexOf('.'));

        try {
            Class<?> mapperClass = Class.forName(className);
            // 1. 先判断类上是否有注解
            NoSqlLog annByClazz = AnnotationUtils.findAnnotation(mapperClass, NoSqlLog.class);
            if (annByClazz != null) {
                return true;
            }
            // TODO: suyh - 当前方法上的注解
            // // 2. 再判断方法上是否有注解
            // for (Method method : ReflectionUtils.getDeclaredMethods(mapperClass)) {
            //     NoSqlLog annByMethod = AnnotationUtils.findAnnotation(method , NoSqlLog.class);
            //     if (annByMethod != null) {
            //         return true;
            //     }
            // }
        } catch (ClassNotFoundException e) {
            log.warn("Mapper class not found: {}", className, e);
        }
        return false;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
    }

    private static String extractUri(String url) {
        try {
            int questionIndex = url.indexOf('?');
            return questionIndex > 0 ? url.substring(0, questionIndex) : url;
        } catch (Exception e) {
            log.error("Error extracting URL without params: {}", url, e);
            return url;
        }
    }
}
