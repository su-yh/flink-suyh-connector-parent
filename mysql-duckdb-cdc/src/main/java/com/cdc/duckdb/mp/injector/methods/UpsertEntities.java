package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.mapping.MappedStatement;

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
        // TODO: suyh - 还没有实现呢！！！
        return null;
    }
}
