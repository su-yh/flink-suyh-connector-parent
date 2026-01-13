package org.apache.doris.flink.tools.cdc;

/**
 * @author suyh
 * @since 2026-01-13
 */
public final class DuckdbType {

    // ---------------------- 布尔类型 ----------------------
    public static final String BOOLEAN = "BOOLEAN";

    // ---------------------- 整数类型 ----------------------
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";

    // ---------------------- 浮点类型 ----------------------
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";

    // ---------------------- 高精度小数类型 ----------------------
    public static final String DECIMAL = "DECIMAL";

    // ---------------------- 日期时间类型 ----------------------
    public static final String DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String TIMESTAMP = "TIMESTAMP";

    // ---------------------- 字符串类型 ----------------------
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";

    // ---------------------- 二进制类型 ----------------------
    public static final String BLOB = "BLOB";

    // ---------------------- 特殊类型 ----------------------
    public static final String JSON = "JSON";

    // ---------------------- 私有构造方法 ----------------------
    /**
     * 私有构造方法，禁止实例化该常量类
     * 避免创建无用的类实例，符合常量类的设计规范
     */
    private DuckdbType() {
        throw new UnsupportedOperationException("This is a constant class and cannot be instantiated.");
    }
}
