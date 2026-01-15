package com.cdc.duckdb.mp.config;

import com.baomidou.mybatisplus.core.injector.ISqlInjector;
import com.baomidou.mybatisplus.extension.plugins.inner.OptimisticLockerInnerInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import com.cdc.duckdb.mp.injector.GlobalSqlInjector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author suyh
 * @since 2025-08-28
 */
@Configuration
public class MybatisPlusConfiguration {
    /**
     * 分页插件
     */
    @Bean
    public PaginationInnerInterceptor paginationInnerInterceptor() {
        return new PaginationInnerInterceptor();
    }

    @Bean
    public OptimisticLockerInnerInterceptor optimisticLockerInnerInterceptor() {
        return new OptimisticLockerInnerInterceptor();
    }

    // 自定义注入实现
    @Bean
    public ISqlInjector sqlInjector() {
        return new GlobalSqlInjector();
    }

    // @Bean
    // public SqlHandler sqlHandler() {
    //     SqlHandler.PLUS_LOG_ENABLED = true;
    //     return new SqlHandler();
    // }
}
