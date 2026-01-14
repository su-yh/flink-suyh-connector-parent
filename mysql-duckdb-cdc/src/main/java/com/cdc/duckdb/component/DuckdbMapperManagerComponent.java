package com.cdc.duckdb.component;

import com.cdc.duckdb.mp.ann.TbColumn;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.duckdb.sink.DuckDBWriter;
import org.apache.duckdb.sink.RecordDto;
import org.apache.ibatis.binding.MapperRegistry;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author suyh
 * @since 2026-01-14
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DuckdbMapperManagerComponent {
    private final SqlSessionFactory sqlSessionFactory;
    private final GenericApplicationContext genericApplicationContext;
    // 每一张表对应的Entity
    private final Map<String, Class<?>> tableEntityMapping = new ConcurrentHashMap<>();
    // 每一张表对应的spring 容器中的 mapper bean 对象
    private final Map<String, BaseMapperDuckdb<?>> mapperBeanMapping = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        DuckDBWriter.duckdbMapperManagerComponent = this;
    }

    public void registerMapperBean(SourceSchema schema) throws Exception {
        String tableName = schema.getTableName();

        Class<?> entityClass = JavassistDynamicClassGenerator.generateDynamicEntity(schema, "com.cdc.duckdb.mp.entity", schema.getTableName() + "_entity");
        Class<?> mapperClass = JavassistDynamicClassGenerator.generateDynamicMapper("com.cdc.duckdb.mp.mapper", schema.getTableName() + "_mapper", entityClass);

        Configuration configuration = sqlSessionFactory.getConfiguration();
        MapperRegistry mapperRegistry = configuration.getMapperRegistry();
        mapperRegistry.addMapper(mapperClass); // 注册到MyBatis（必须）

        BaseMapperDuckdb<?> baseMapper = (BaseMapperDuckdb<?>) configuration.getMapper(mapperClass, sqlSessionFactory.openSession(true));

        tableEntityMapping.put(tableName, entityClass);

        String beanName = baseMapper.getClass().getSimpleName();
        genericApplicationContext.registerBean(beanName, BaseMapperDuckdb.class, () -> baseMapper);

        BaseMapperDuckdb<?> baseMapperBean = genericApplicationContext.getBean(beanName, BaseMapperDuckdb.class);
        mapperBeanMapping.put(tableName, baseMapperBean);
    }

    public BaseMapperDuckdb<?> getMapperBean(String tableName) {
        return mapperBeanMapping.get(tableName);
    }

    public void upsertEntity(RecordDto recordDto) throws InstantiationException, IllegalAccessException {
        String table = recordDto.getSource().getTable();
        BaseMapperDuckdb<?> baseMapperDuckdb = mapperBeanMapping.get(table);
        Class<?> entityClass = tableEntityMapping.get(table);
        Object entity = entityClass.newInstance();

        entityPropertiesSetter(entityClass, entity, recordDto.getAfter());

        baseMapperDuckdb.upsertObjects(Collections.singletonList(entity));
    }

    private void entityPropertiesSetter(Class<?> modelClass, Object entity, Map<String, Object> properties) throws IllegalAccessException {
        Field[] fields = modelClass.getDeclaredFields();

        for (Field field : fields) {
            // 跳过静态属性
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            // 判断 @TbColumn 注解
            if (field.isAnnotationPresent(TbColumn.class)) {
                TbColumn tbColumn = field.getAnnotation(TbColumn.class);
                if (tbColumn == null) {
                    continue;
                }
            }

            field.setAccessible(true);

            String fieldName = field.getName();
            Object value = properties.get(fieldName);

            if (value != null) {
                Class<?> fieldType = field.getType();
                if (BigDecimal.class.equals(fieldType)) {
                    if (value instanceof Number) {
                        Number numberValue = (Number) value;
                        value = BigDecimal.valueOf(numberValue.doubleValue());
                    } else if (value instanceof String) {
                        try {
                            String strValue = (String) value;
                            value = new BigDecimal(strValue);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("字符串无法转换为有效BigDecimal，字段名：" + field.getName() + "，待转换值：" + value, e);
                        }
                    }
                }
            }

            try {
                field.set(entity, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("字段赋值失败，字段名：" + fieldName + "，目标类型：" + field.getType().getName() + "，值类型：" + (value != null ? value.getClass().getName() : "null"), e);
            }
        }

        Class<?> superClass = modelClass.getSuperclass();
        if (superClass != null && superClass != Object.class) {
            entityPropertiesSetter(superClass, entity, properties);
        }
    }
}
