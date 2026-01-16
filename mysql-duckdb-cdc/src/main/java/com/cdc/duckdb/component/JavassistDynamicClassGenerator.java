package com.cdc.duckdb.component;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.cdc.duckdb.mp.ann.TbColumn;
import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.Modifier;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.StringMemberValue;
import org.apache.doris.flink.catalog.doris.DuckdbFieldSchema;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.ibatis.javassist.bytecode.SignatureAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JavassistDynamicClassGenerator {

    /**
     * 基于 MysqlSchema 动态生成 MyBatis-Plus Entity 类（修改后：实现 BaseEntity 并实现 getPrimaryKey()）
     * @param sourceSchema Doris CDC 提供的 MySQL 表结构元数据
     * @param entityPackage 生成Entity的包名（如：com.ebusiness.entity）
     * @param entityClassName 生成Entity的类名（如：PersonEntity）
     * @return 动态生成的Entity Class对象
     * @throws Exception 生成过程中的异常（Javassist操作、反射相关）
     */
    public static Class<?> generateDynamicEntity(SourceSchema sourceSchema, String entityPackage, String entityClassName) throws Exception {
        // 1. 初始化 Javassist ClassPool（类池，用于创建/获取CtClass）
        ClassPool classPool = ClassPool.getDefault();
        classPool.importPackage(TableName.class.getName());
        classPool.importPackage(IdType.class.getName());
        classPool.importPackage(BaseEntity.class.getName());

        // 2. 构建完整的类名（包名+类名）
        String fullEntityClassName = entityPackage + "." + entityClassName;
        CtClass ctEntityClass = classPool.makeClass(fullEntityClassName);

        CtClass baseEntityCt = classPool.get(BaseEntity.class.getName());
        ctEntityClass.addInterface(baseEntityCt);

        // 4. 为Entity添加 MyBatis-Plus @TableName 注解（关联数据库表名）
        String tableName = sourceSchema.getTableName();
        addTableNameAnnotation(ctEntityClass, tableName);

        // 5. 遍历 MysqlSchema 中的列信息，动态生成Entity的成员变量及注解
        Map<String, DuckdbFieldSchema> fields = sourceSchema.getDuckdbFields();
        List<DuckdbFieldSchema> mysqlColumns = fields == null ? new ArrayList<>() : new ArrayList<>(fields.values());
        for (DuckdbFieldSchema column : mysqlColumns) {
            generateEntityField(ctEntityClass, column);
            // 查找主键字段（基于 DuckdbFieldSchema.isPrimaryKey() 判断）
            if (column.isPrimaryKey()) {
                // TODO: 在这里补充，实现接口方法，方法名我修改成了：Object primaryKey();
                //  这里只需要在子类实现它，并把 主键的字段名返回就可以了
                //  public Object primaryKey() { return ${column.getName()}; }
            }
        }

        ctEntityClass.writeFile("./debug");

        // 6. 将 CtClass 转换为实际的 Class 对象并返回
        return ctEntityClass.toClass();
    }

    /**
     * 为动态类添加 @TableName 注解（指定数据库表名）
     */
    private static void addTableNameAnnotation(CtClass ctClass, String tableName) {
        ConstPool constPool = ctClass.getClassFile().getConstPool();
        AnnotationsAttribute tableNameAnnotationAttr = new AnnotationsAttribute(constPool, AnnotationsAttribute.visibleTag);
        Annotation tableNameAnnotation = new Annotation(TableName.class.getName(), constPool);
        // 设置 @TableName 的 value 属性（对应数据库表名）
        StringMemberValue tableNameValue = new StringMemberValue(constPool);
        tableNameValue.setValue(tableName);
        tableNameAnnotation.addMemberValue("value", tableNameValue);
        tableNameAnnotationAttr.addAnnotation(tableNameAnnotation);
        // 将注解添加到类上
        ctClass.getClassFile().addAttribute(tableNameAnnotationAttr);
    }

    /**
     * 基于 MysqlColumn 动态生成Entity的成员变量
     */
    private static void generateEntityField(CtClass ctEntityClass, DuckdbFieldSchema mysqlColumn) throws Exception {
        ClassPool classPool = ctEntityClass.getClassPool();
        ConstPool constPool = ctEntityClass.getClassFile().getConstPool();

        // 1. 映射 MySQL 字段类型到 Java 类型（简化版，可根据实际需求扩展更多类型）
        CtClass fieldType = classPool.get(mysqlColumn.getJavaClazz().getName());
        if (fieldType == null) {
            throw new IllegalArgumentException("不支持的MySQL字段类型：" + mysqlColumn.getTypeString());
        }

        // 属性名直接使用表字段名，并且这里全是小写的，在初始化时就固定了。
        String fieldName = mysqlColumn.getName();
        CtField ctField = new CtField(fieldType, fieldName, ctEntityClass);

        // 3. 设置成员变量修饰符为 private（符合JavaBean规范）
        ctField.setModifiers(Modifier.PRIVATE);

        Annotation tableFieldAnnotationAttr = buildTableFieldAnnotation(constPool, mysqlColumn.getName());
        Annotation tbColumnAnnotation = buildTbColumnAnnotation(constPool, mysqlColumn);
        AnnotationsAttribute fieldAnnotationAttr = new AnnotationsAttribute(constPool, AnnotationsAttribute.visibleTag);
        fieldAnnotationAttr.addAnnotation(tableFieldAnnotationAttr);
        fieldAnnotationAttr.addAnnotation(tbColumnAnnotation);
        ctField.getFieldInfo().addAttribute(fieldAnnotationAttr);

        // 4. 将成员变量添加到动态类中
        ctEntityClass.addField(ctField);

        // 5. 为成员变量生成 getter/setter 方法（若不使用Lombok，可手动生成；使用Lombok可省略，这里做兼容）
        generateGetterSetter(ctEntityClass, ctField, fieldName, fieldType);
    }

    private static Annotation buildTableFieldAnnotation(ConstPool constPool, String dbColumnName) {
        String tableFieldAnnotationClass = TableField.class.getName();
        Annotation tableFieldAnnotation = new Annotation(tableFieldAnnotationClass, constPool);
        StringMemberValue valueMember = new StringMemberValue(dbColumnName, constPool);
        tableFieldAnnotation.addMemberValue("value", valueMember);
        return tableFieldAnnotation;
    }

    private static Annotation buildTbColumnAnnotation(ConstPool constPool, DuckdbFieldSchema mysqlColumn) {
        String tbColumnAnnotationClass = TbColumn.class.getName();
        Annotation tbColumnAnnotation = new Annotation(tbColumnAnnotationClass, constPool);

        // 示例：@TbColumn(value = "id", type = "BIGINT", primaryKey = true)
        StringMemberValue valueMember = new StringMemberValue(mysqlColumn.getName(), constPool);
        tbColumnAnnotation.addMemberValue("value", valueMember);
        StringMemberValue typeMember = new StringMemberValue(mysqlColumn.getTypeString(), constPool);
        tbColumnAnnotation.addMemberValue("type", typeMember);
        BooleanMemberValue primaryKeyMember = new BooleanMemberValue(mysqlColumn.isPrimaryKey(), constPool);
        tbColumnAnnotation.addMemberValue("primaryKey", primaryKeyMember);
        BooleanMemberValue enableMember = new BooleanMemberValue(true, constPool);
        tbColumnAnnotation.addMemberValue("enable", enableMember);

        return tbColumnAnnotation;
    }

    /**
     * 手动生成 getter/setter 方法（兼容不使用Lombok的场景）
     */
    private static void generateGetterSetter(CtClass ctClass, CtField ctField, String fieldName, CtClass fieldType) throws Exception {
        // 生成 getter 方法（如：public Long getId() { return id; }）
        String getterName = "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        CtMethod getterMethod = new CtMethod(fieldType, getterName, new CtClass[]{}, ctClass);
        getterMethod.setModifiers(Modifier.PUBLIC);
        getterMethod.setBody("return this." + fieldName + ";");
        ctClass.addMethod(getterMethod);

        // 生成 setter 方法（如：public void setId(Long id) { this.id = id; }）
        String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        CtMethod setterMethod = new CtMethod(CtClass.voidType, setterName, new CtClass[]{fieldType}, ctClass);
        setterMethod.setModifiers(Modifier.PUBLIC);
        setterMethod.setBody("this." + fieldName + " = $1;");
        ctClass.addMethod(setterMethod);
    }

    /**
     * 原有动态 Mapper 生成方法（优化：支持传入动态生成的Entity Class）
     */
    public static Class<?> generateDynamicMapper(String mapperPackage, String mapperClassName, Class<?> entityClass) {
        try {
            ClassPool pool = ClassPool.getDefault();

            // 1. 获取 BaseMapperDuckdb 的 CtClass
            CtClass baseMapperCt = pool.get(BaseMapperDuckdb.class.getName());

            // 2. 构建 Mapper 接口的完整类名
            String fullMapperClassName = mapperPackage + "." + mapperClassName;
            CtClass mapperCt = pool.makeInterface(fullMapperClassName, baseMapperCt);

            // 3. 构建泛型签名（绑定动态生成的Entity）
            SignatureAttribute.ClassSignature ac = new SignatureAttribute.ClassSignature(
                    null, null,
                    new SignatureAttribute.ClassType[]{
                            new SignatureAttribute.ClassType(BaseMapperDuckdb.class.getName(),
                                    new SignatureAttribute.TypeArgument[]{
                                            new SignatureAttribute.TypeArgument(
                                                    new SignatureAttribute.ClassType(entityClass.getName())
                                            )
                                    })
                    });

            // 4. 设置泛型签名并转换为 Class
            mapperCt.setGenericSignature(ac.encode());

            return mapperCt.toClass();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

