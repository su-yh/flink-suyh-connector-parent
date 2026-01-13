package com.cdc.duckdb.component;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.Modifier;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.StringMemberValue;
import lombok.Data;
import org.apache.doris.flink.catalog.doris.DuckdbFieldSchema;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.ibatis.javassist.bytecode.SignatureAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JavassistDynamicClassGenerator {

    // 原有硬编码Entity（保留用于对比，可注释）
    @Data
    @TableName("user_info")
    public static class UserInfoEntity {
        @TableId(value = "id", type = IdType.AUTO)
        private Long id;

        private Long traceId;
    }

    /**
     * 基于 MysqlSchema 动态生成 MyBatis-Plus Entity 类
     * @param sourceSchema Doris CDC 提供的 MySQL 表结构元数据
     * @param entityPackage 生成Entity的包名（如：com.ebusiness.entity）
     * @param entityClassName 生成Entity的类名（如：PersonEntity）
     * @return 动态生成的Entity Class对象
     * @throws Exception 生成过程中的异常（Javassist操作、反射相关）
     */
    public static Class<?> generateDynamicEntity(SourceSchema sourceSchema, String entityPackage, String entityClassName) throws Exception {
        // 1. 初始化 Javassist ClassPool（类池，用于创建/获取CtClass）
        ClassPool classPool = ClassPool.getDefault();
        // 补充Lombok和MyBatis-Plus的类路径（避免类找不到）
        classPool.importPackage("lombok.Data");
        classPool.importPackage("com.baomidou.mybatisplus.annotation.TableName");
        classPool.importPackage("com.baomidou.mybatisplus.annotation.IdType");

        // 2. 构建完整的类名（包名+类名）
        String fullEntityClassName = entityPackage + "." + entityClassName;
        CtClass ctEntityClass = classPool.makeClass(fullEntityClassName);

        // 3. 为Entity添加 Lombok @Data 注解（自动生成getter/setter/toString等）
        addDataAnnotation(ctEntityClass);

        // 4. 为Entity添加 MyBatis-Plus @TableName 注解（关联数据库表名）
        String tableName = sourceSchema.getTableName();
        addTableNameAnnotation(ctEntityClass, tableName);

        // 5. 遍历 MysqlSchema 中的列信息，动态生成Entity的成员变量及注解
        Map<String, FieldSchema> fields = sourceSchema.getFields();
        List<FieldSchema> mysqlColumns = fields == null ? new ArrayList<>() : new ArrayList<>(fields.values());
        for (FieldSchema column : mysqlColumns) {
            generateEntityField(ctEntityClass, (DuckdbFieldSchema) column);
        }

        ctEntityClass.writeFile("./debug"); // TODO: suyh - 测试，验证结果。这是会生成java 文件，还是生成class 文件

        // 6. 将 CtClass 转换为实际的 Class 对象并返回
        return ctEntityClass.toClass();
    }

    /**
     * 为动态类添加 @Data 注解
     */
    private static void addDataAnnotation(CtClass ctClass) {
        ConstPool constPool = ctClass.getClassFile().getConstPool();
        // 创建 @Data 注解对象
        AnnotationsAttribute dataAnnotationAttr = new AnnotationsAttribute(constPool, AnnotationsAttribute.visibleTag);
        Annotation dataAnnotation = new Annotation(Data.class.getName(), constPool);
        dataAnnotationAttr.addAnnotation(dataAnnotation);
        // 将注解添加到类上
        ctClass.getClassFile().addAttribute(dataAnnotationAttr);
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

        // 2. 构建 Java 成员变量名（默认：数据库字段名下划线转驼峰，如 user_id -> userId）
        String fieldName = underlineToCamel(mysqlColumn.getName());
        // String fieldName = mysqlColumn.getName();
        CtField ctField = new CtField(fieldType, fieldName, ctEntityClass);

        // 3. 设置成员变量修饰符为 private（符合JavaBean规范）
        ctField.setModifiers(Modifier.PRIVATE);

        // 5. 将成员变量添加到动态类中
        ctEntityClass.addField(ctField);

        // 6. 为成员变量生成 getter/setter 方法（若不使用Lombok，可手动生成；使用Lombok可省略，这里做兼容）
        generateGetterSetter(ctEntityClass, ctField, fieldName, fieldType);
    }

    /**
     * 下划线命名转驼峰命名（如 user_info -> userInfo）
     */
    private static String underlineToCamel(String underlineName) {
        if (underlineName == null || !underlineName.contains("_")) {
            return underlineName;
        }
        StringBuilder camelBuilder = new StringBuilder();
        boolean nextUpper = false;
        for (char c : underlineName.toCharArray()) {
            if (c == '_') {
                nextUpper = true;
            } else {
                if (nextUpper) {
                    camelBuilder.append(Character.toUpperCase(c));
                    nextUpper = false;
                } else {
                    camelBuilder.append(Character.toLowerCase(c));
                }
            }
        }
        return camelBuilder.toString();
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
    public static Class<?> generateDynamicMapper(Class<?> entityClass) {
        try {
            ClassPool pool = ClassPool.getDefault();

            // 1. 获取 BaseMapper 的 CtClass
            CtClass baseMapperCt = pool.get(BaseMapper.class.getName());

            // 2. 构建 Mapper 接口的完整类名
            String mapperClassName = "com.ebusiness.mp.mysql.base.mapper.custom.PersonCustomMapper";
            CtClass mapperCt = pool.makeInterface(mapperClassName, baseMapperCt);

            // 3. 构建泛型签名（绑定动态生成的Entity）
            SignatureAttribute.ClassSignature ac = new SignatureAttribute.ClassSignature(
                    null, null,
                    new SignatureAttribute.ClassType[]{
                            new SignatureAttribute.ClassType(BaseMapper.class.getName(),
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

    // 重载方法：兼容原有硬编码 Entity
    public static Class<?> generateDynamicMapper() {
        return generateDynamicMapper(UserInfoEntity.class);
    }
}
