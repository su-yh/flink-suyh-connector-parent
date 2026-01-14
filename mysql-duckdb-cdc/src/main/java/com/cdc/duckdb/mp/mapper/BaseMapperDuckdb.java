package com.cdc.duckdb.mp.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cdc.duckdb.config.datasource.DataSourceNames;

import java.util.Collection;

/**
 * @author suyh
 * @since 2026-01-14
 */
@DS(DataSourceNames.STATISTICAL_ANALYSIS_DUCK)
public interface BaseMapperDuckdb<T> extends BaseMapper<T> {
    void createTableIfNotExists();
    void upsertEntities(Collection<T> entities);
}


