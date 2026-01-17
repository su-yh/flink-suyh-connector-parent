package com.cdc.duckdb.component;

import com.cdc.duckdb.mp.ann.TbColumn;
import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.duckdb.sink.DuckDBWriter;
import org.apache.duckdb.sink.JsonUtils;
import org.apache.duckdb.sink.RecordDto;
import org.apache.ibatis.binding.MapperRegistry;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
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
    private final SqlSessionTemplate sqlSessionTemplate;
    private final GenericApplicationContext genericApplicationContext;
    // 每一张表对应的Entity
    private final Map<String, Class<? extends BaseEntity>> tableEntityMapping = new ConcurrentHashMap<>();
    // 每一张表对应的spring 容器中的 mapper bean 对象
    private final Map<String, BaseMapperDuckdb<?>> mapperBeanMapping = new ConcurrentHashMap<>();
    private CdcConcurrentThreads cdcConcurrentThreads;

    @PostConstruct
    public void init() {
        DuckDBWriter.duckdbMapperManagerComponent = this;

        if (cdcConcurrentThreads == null) {
            cdcConcurrentThreads = new CdcConcurrentThreads(mapperBeanMapping::get);
            cdcConcurrentThreads.init();
        }
    }

    public void registerMapperBean(SourceSchema schema) throws Exception {
        String tableName = schema.getTableName();
        Assert.hasText(tableName, "表名不能为空");

        // 1. 动态生成 Entity 和 Mapper 类（保持不变）
        Class<? extends BaseEntity> entityClass = JavassistDynamicClassGenerator.generateDynamicEntity(schema, "com.cdc.duckdb.mp.entity", schema.getTableName() + "_entity");
        Class<?> mapperClass = JavassistDynamicClassGenerator.generateDynamicMapper("com.cdc.duckdb.mp.mapper", schema.getTableName() + "_mapper", entityClass);

        // 2. 注册 Mapper 到 MyBatis 注册表（必须，让 MyBatis 识别 Mapper 接口和 SQL 定义）
        Configuration configuration = sqlSessionFactory.getConfiguration();
        MapperRegistry mapperRegistry = configuration.getMapperRegistry();
        mapperRegistry.addMapper(mapperClass);

        BaseMapperDuckdb<?> baseMapper = (BaseMapperDuckdb<?>) sqlSessionTemplate.getMapper(mapperClass);

        // 4. 验证 Mapper 对象有效性
        Assert.notNull(baseMapper, "动态生成 Mapper 代理对象失败，表名：" + tableName);

        // 5. 存储 Entity 类映射（后续实例化使用，保持不变）
        tableEntityMapping.put(tableName, entityClass);

        // 6. 注册 Mapper 到 Spring 容器，由 Spring 托管（后续复用该 Bean，无连接泄露风险）
        String beanName = mapperClass.getSimpleName(); // 用 Mapper 接口类名作为 Bean 名，更规范
        genericApplicationContext.registerBean(beanName, BaseMapperDuckdb.class, () -> baseMapper);

        // 7. 从 Spring 容器获取 Mapper Bean，存储到映射表（供后续业务使用）
        BaseMapperDuckdb<?> baseMapperBean = genericApplicationContext.getBean(beanName, BaseMapperDuckdb.class);
        mapperBeanMapping.put(tableName, baseMapperBean);
    }

    public BaseMapperDuckdb<?> getMapperBean(String tableName) {
        return mapperBeanMapping.get(tableName);
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
                } else if (Long.class.equals(fieldType)) {
                    if (value instanceof Integer) {
                        Integer intValue = (Integer) value;
                        value = intValue.longValue();
                    } else if (value instanceof Number) {
                        Number numberValue = (Number) value;
                        value = numberValue.longValue();
                    } else if (value instanceof String) {
                        try {
                            String strValue = (String) value;
                            value = Long.valueOf(strValue);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("字符串无法转换为有效Long，字段名：" + field.getName() + "，待转换值：" + value, e);
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

    // 读和写都要允许阻塞，直到成功为止，不然flink 的checkpoint 将会出现问题。
    public void write(RecordDto recordDto) throws InterruptedException {
        String duckdbTbName = mappingDuckdbTbName(recordDto);
        try {
            BaseEntity entity = mappingEntity(recordDto);
            cdcConcurrentThreads.write(duckdbTbName, entity);
        } catch (InstantiationException | IllegalAccessException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void syncFlush() {
        cdcConcurrentThreads.syncFlush();
    }

    private String mappingDuckdbTbName(RecordDto recordDto) {
        // TODO: suyh - 待处理
        //   测试，暂时处理成mysql 的表名
        return recordDto.getSource().getTable();
    }

    private BaseEntity mappingEntity(RecordDto recordDto) throws InstantiationException, IllegalAccessException, JsonProcessingException {
        String table = recordDto.getSource().getTable();
        Class<? extends BaseEntity> entityClass = tableEntityMapping.get(table);
        return JsonUtils.deserialize(recordDto.getAfterJson(), entityClass);
    }


}
