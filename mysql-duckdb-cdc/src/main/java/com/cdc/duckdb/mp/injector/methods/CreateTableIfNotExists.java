package com.cdc.duckdb.mp.injector.methods;

import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.cdc.duckdb.mp.ann.TbColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Slf4j
public class CreateTableIfNotExists extends AbstractMethod {
    // SqlMethod
    private static final String METHOD = "createTableIfNotExists";
    private static final String DESC = "当表不存在时创建表";

    public CreateTableIfNotExists() {
        super(METHOD);
    }

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        String tableName = tableInfo.getTableName();

        List<TbColumn> columns = new ArrayList<>();
        extractDuckdbColumnType(columns, modelClass);

        String sql = buildCreateTableSql(tableName, columns);
        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sql, null);
        return this.addInsertMappedStatement(
                mapperClass, modelClass, super.methodName, sqlSource,
                NoKeyGenerator.INSTANCE, null, null
        );
    }

    private static String buildCreateTableSql(String tableName, List<TbColumn> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("<script>CREATE TABLE IF NOT EXISTS ");
        sb.append(tableName);
        sb.append("(");

        List<String> columDefine = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        for (TbColumn column : columns) {
            if (!column.enable()) {
                continue;
            }

            columDefine.add('"' + column.value() + '"' + " " + column.type());

            if (column.primaryKey()) {
                primaryKeys.add(column.value());
            }
        }

        sb.append(String.join(",", columDefine));

        if (!primaryKeys.isEmpty()) {
            sb.append(", PRIMARY KEY(");
            sb.append(String.join(",", primaryKeys));
            sb.append(")");
        }

        sb.append(")");
        sb.append("</script>");

        return sb.toString();
    }

    public static void extractDuckdbColumnType(List<TbColumn> columns, Class<?> modelClass) {
        // 校验参数非空
        if (modelClass == null) {
            throw new IllegalArgumentException("modelClass 不能为 null");
        }

        // 步骤1：获取当前类中所有声明的属性（包括 private、protected，不包括父类继承的属性）
        Field[] fields = modelClass.getDeclaredFields();

        // 步骤2：遍历当前类的所有属性
        for (Field field : fields) {
            // 优化点1：跳过静态属性（判断字段是否包含 static 修饰符）
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                continue; // 直接跳过，不处理静态属性
            }

            // 步骤3：判断当前属性是否存在 @TbColumn 注解
            if (field.isAnnotationPresent(TbColumn.class)) {
                // 步骤4：获取注解实例（无需手动处理权限，注解获取不受属性访问修饰符影响）
                TbColumn tbColumn = field.getAnnotation(TbColumn.class);
                columns.add(tbColumn);
            }
        }

        // 优化点2：递归处理父类（获取父类 Class，递归调用当前方法）
        Class<?> superClass = modelClass.getSuperclass();
        // 递归终止条件：1. 父类为 null；2. 父类是 Object 类（无实际业务属性）
        if (superClass != null && superClass != Object.class) {
            extractDuckdbColumnType(columns, superClass); // 递归调用，处理父类属性
        }
    }

}
