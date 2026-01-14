package com.cdc.duckdb.mp.ann;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface TbColumn {

    /**
     * 字段名
     */
    String value();

    boolean enable() default true;

    /**
     * 字段数据类型（如 VARCHAR(32)、BIGINT、DECIMAL(18,2)）
     */
    String type();

    /**
     * 字段注释
     */
    String comment() default "";

    /**
     * 是否为主键
     */
    boolean primaryKey() default false;
}
