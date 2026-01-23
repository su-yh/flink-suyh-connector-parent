package com.cdc.duckdb.mp.injector;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.injector.DefaultSqlInjector;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.cdc.duckdb.mp.injector.methods.CreateTableIfNotExists;
import com.cdc.duckdb.mp.injector.methods.ExecuteDdlSql;
import com.cdc.duckdb.mp.injector.methods.InsertEntities;
import com.cdc.duckdb.mp.injector.methods.TruncateTable;
import com.cdc.duckdb.mp.injector.methods.UpsertEntities;

import java.util.List;

/**
 * @author suyh
 * @since 2025-08-28
 */
public class GlobalSqlInjector extends DefaultSqlInjector {
    @Override
    public List<AbstractMethod> getMethodList(Class<?> mapperClass, TableInfo tableInfo) {
        List<AbstractMethod> methodList = super.getMethodList(mapperClass, tableInfo);
        methodList.add(new ExecuteDdlSql());
        methodList.add(new TruncateTable());
        methodList.add(new CreateTableIfNotExists());
        methodList.add(new UpsertEntities());
        methodList.add(new InsertEntities());
        return methodList;
    }
}

