package org.apache.doris.flink.catalog.doris;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author suyh
 * @since 2026-01-13
 */
@NoArgsConstructor
@Data
public class DuckdbFieldSchema extends FieldSchema {
    private Class<?> javaClazz;
    private boolean primaryKey; // TODO: suyh - 是否主键

    public DuckdbFieldSchema(String name, String typeString, String comment, Class<?> javaClazz) {
        super(name, typeString, comment);

        this.javaClazz = javaClazz;
    }
}
