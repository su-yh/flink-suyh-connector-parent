package com.cdc.duckdb.mp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;

/**
 * @author suyh
 * @since 2026-01-14
 */
public interface BaseMapperDuckdb<T> extends BaseMapper<T> {
    void createTableIfNotExists();
}
