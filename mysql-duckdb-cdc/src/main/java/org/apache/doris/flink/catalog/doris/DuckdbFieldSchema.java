package org.apache.doris.flink.catalog.doris;

import lombok.Data;

/**
 * @author suyh
 * @since 2026-01-13
 */
@Data
public class DuckdbFieldSchema extends FieldSchema {
    private final Class<?> javaClazz;
    private final boolean primaryKey; // TODO: suyh - 是否主键

    public DuckdbFieldSchema(String name, String typeString, String comment, Class<?> javaClazz, boolean primaryKey) {
        super(name, typeString, comment);

        this.javaClazz = javaClazz;
        this.primaryKey = primaryKey;
    }
}
