package org.apache.duckdb.sink.mp;

import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author suyh
 * @since 2026-01-12
 */
@Data
public class CommonTableMeta {
    // 表名
    private String tableName;
    // 主键字段名
    private String pkColumn;
    // 字段名 -> 数据库字段类型（VARCHAR/INT/BIGINT等）
    private Map<String, String> columnDbTypeMap;
    // 字段名 -> Java类型（java.lang.String/java.lang.Integer等，用于动态生成Entity属性）
    private Map<String, String> columnJavaTypeMap;
    // 所有有效字段列表（含主键）
    private List<String> allColumns;
    // 是否自增主键
    private boolean autoIncrementPk;

    // 全局表元数据缓存
    public static final Map<String, CommonTableMeta> TABLE_META_CACHE = new ConcurrentHashMap<>();

    // 初始化表元数据（包含数据库类型 → Java类型的映射）
    static {
        // 示例：t_user表
        CommonTableMeta userTableMeta = new CommonTableMeta();
        userTableMeta.setTableName("t_user");
        userTableMeta.setPkColumn("id");
        userTableMeta.setAutoIncrementPk(true);

        // 数据库字段类型映射
        Map<String, String> dbTypeMap = new ConcurrentHashMap<>();
        dbTypeMap.put("id", "BIGINT");
        dbTypeMap.put("name", "VARCHAR");
        dbTypeMap.put("age", "INT");
        dbTypeMap.put("create_time", "DATETIME");

        // Java类型映射（关键：用于动态生成Entity的属性类型）
        Map<String, String> javaTypeMap = new ConcurrentHashMap<>();
        javaTypeMap.put("id", "java.lang.Long");
        javaTypeMap.put("name", "java.lang.String");
        javaTypeMap.put("age", "java.lang.Integer");
        javaTypeMap.put("create_time", "java.lang.String"); // 简化处理，也可使用java.util.Date

        userTableMeta.setColumnDbTypeMap(dbTypeMap);
        userTableMeta.setColumnJavaTypeMap(javaTypeMap);
        userTableMeta.setAllColumns(Arrays.asList("id", "name", "age", "create_time"));

        TABLE_META_CACHE.put("t_user", userTableMeta);
    }

    // 工具方法：根据表名获取表元数据
    public static CommonTableMeta getTableMeta(String tableName) {
        CommonTableMeta tableMeta = TABLE_META_CACHE.get(tableName);
        if (tableMeta == null) {
            throw new IllegalArgumentException("未知表名：" + tableName + "，请先配置表元数据！");
        }
        return tableMeta;
    }

    // 数据库类型 → Java类型的默认映射（可扩展）
    public static String getDefaultJavaType(String dbType) {
        // 先做非空判断（可选但更健壮，避免空指针异常）
        if (dbType == null) {
            return "java.lang.Object";
        }
        // 转换为大写，保持原逻辑一致
        String upperDbType = dbType.toUpperCase();
        // JDK8支持的传统switch语句（带break，避免case穿透）
        switch (upperDbType) {
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return "java.lang.String";
            case "INT":
                return "java.math.Integer";
            case "BIGINT":
                return "java.lang.Long";
            case "DATETIME":
            case "DATE":
                return "java.lang.String";
            case "DECIMAL":
                return "java.math.BigDecimal";
            // 默认分支，对应原逻辑的default
            default:
                return "java.lang.Object";
        }
    }
}
