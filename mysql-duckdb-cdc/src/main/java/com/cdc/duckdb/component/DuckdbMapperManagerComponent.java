package com.cdc.duckdb.component;

import com.cdc.duckdb.mp.entity.BaseEntity;
import com.cdc.duckdb.mp.mapper.BaseMapperDuckdb;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.data.Envelope;
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
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
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
    // 每一张表(duckdbTableName)对应的Entity
    private final Map<String, Class<? extends BaseEntity>> tableEntityMapping = new ConcurrentHashMap<>();
    // 每一张表(duckdbTableName)对应的spring 容器中的 mapper bean 对象
    private final Map<String, BaseMapperDuckdb<?>> mapperBeanMapping = new ConcurrentHashMap<>();
    private CdcConcurrentThreads cdcConcurrentThreads;

    @PostConstruct
    public void init() {
        DuckDBWriter.duckdbMapperManagerComponent = this;

        if (cdcConcurrentThreads == null) {
            cdcConcurrentThreads = new CdcConcurrentThreads();
            cdcConcurrentThreads.init();
        }
    }

    public void registerMapperBean(SourceSchema schema) throws Exception {
        log.info("registerMapperBean enter.");
        String tableName = schema.getTableName();
        Assert.hasText(tableName, "表名不能为空");
        String duckdbTableName = mappingDuckdbTbName(tableName);

        // 1. 动态生成 Entity 和 Mapper 类（保持不变）
        Class<? extends BaseEntity> entityClass = JavassistDynamicClassGenerator.generateDynamicEntity(schema, "com.cdc.duckdb.mp.entity", duckdbTableName);
        Class<?> mapperClass = JavassistDynamicClassGenerator.generateDynamicMapper("com.cdc.duckdb.mp.mapper", duckdbTableName, entityClass);

        // 2. 注册 Mapper 到 MyBatis 注册表（必须，让 MyBatis 识别 Mapper 接口和 SQL 定义）
        Configuration configuration = sqlSessionFactory.getConfiguration();
        MapperRegistry mapperRegistry = configuration.getMapperRegistry();
        mapperRegistry.addMapper(mapperClass);

        log.info("instance mapper instance.");
        BaseMapperDuckdb<?> baseMapper = (BaseMapperDuckdb<?>) sqlSessionTemplate.getMapper(mapperClass);

        // 4. 验证 Mapper 对象有效性
        Assert.notNull(baseMapper, "动态生成 Mapper 代理对象失败，源表名：" + tableName + ", duckdbTableName: " + duckdbTableName);

        // 5. 存储 Entity 类映射（后续实例化使用，保持不变）
        tableEntityMapping.put(duckdbTableName, entityClass);

        // 6. 注册 Mapper 到 Spring 容器，由 Spring 托管（后续复用该 Bean，无连接泄露风险）
        String beanName = mapperClass.getSimpleName(); // 用 Mapper 接口类名作为 Bean 名，更规范
        log.info("registerBean mapper instance.");
        genericApplicationContext.registerBean(beanName, BaseMapperDuckdb.class, () -> baseMapper);

        // 7. 从 Spring 容器获取 Mapper Bean，存储到映射表（供后续业务使用）
        BaseMapperDuckdb<?> baseMapperBean = genericApplicationContext.getBean(beanName, BaseMapperDuckdb.class);
        mapperBeanMapping.put(duckdbTableName, baseMapperBean);

        if (cdcConcurrentThreads != null) {
            log.info("cdcConcurrentThreads.register.");
            cdcConcurrentThreads.register(duckdbTableName, baseMapperBean);
        }
    }

    public BaseMapperDuckdb<?> getMapperBean(String tableName) {
        String duckdbTableName = mappingDuckdbTbName(tableName);
        return mapperBeanMapping.get(duckdbTableName);
    }

    public void ddl(RecordDto recordDto) {
        String duckdbTbName = mappingDuckdbTbName(recordDto.getSource().getTable());
        cdcConcurrentThreads.ddl(duckdbTbName, recordDto);
    }

    // 读和写都要允许阻塞，直到成功为止，不然flink 的checkpoint 将会出现问题。
    public void write(RecordDto recordDto) throws InterruptedException {
        String duckdbTbName = mappingDuckdbTbName(recordDto.getSource().getTable());
        try {
            BaseEntity entity = mappingEntity(recordDto);
            if (entity == null) {
                return;
            }
            cdcConcurrentThreads.write(recordDto.getOperation(), duckdbTbName, entity);
        } catch (InstantiationException | IllegalAccessException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void syncFlush() {
        cdcConcurrentThreads.syncFlush();
    }

    private String mappingDuckdbTbName(String mysqlTableName) {
        // TODO: suyh - 待处理
        //   测试，暂时处理成mysql 的表名
        return "prefix_" + mysqlTableName;
    }

    private BaseEntity mappingEntity(RecordDto recordDto) throws InstantiationException, IllegalAccessException, JsonProcessingException {
        String tableName = recordDto.getSource().getTable();
        String duckdbTableName = mappingDuckdbTbName(tableName);
        String op = recordDto.getOperation();
        String jsonText;
        if (op.equals(Envelope.Operation.CREATE.code()) || op.equals(Envelope.Operation.UPDATE.code())) {
            log.trace("mappingEntity create|update event");
            jsonText = recordDto.getAfterJson();
        } else if (op.equals(Envelope.Operation.DELETE.code()) || op.equals(Envelope.Operation.TRUNCATE.code())) {
            log.trace("mappingEntity delete|truncate event");
            jsonText = recordDto.getBeforeJson();
        } else if (op.equals(Envelope.Operation.READ.code())) {
            // 全量同步阶段
            log.trace("mappingEntity read event.");
            jsonText = recordDto.getAfterJson();
        } else {
            throw new RuntimeException("UNKNOWN operation: " + op);
        }

        if (!StringUtils.hasText(jsonText)) {
            log.info("jsonText is empty, op: {}, recordDto: {}", op, recordDto);
            return null;
        }

        Class<? extends BaseEntity> entityClass = tableEntityMapping.get(duckdbTableName);
        if (entityClass == null) {
            log.error("COUNT FOUND {}, source table name: {}, duckdb table name: {}", BaseEntity.class.getSimpleName(), tableName, duckdbTableName);
            return null;
        }
        return JsonUtils.deserialize(jsonText, entityClass);
    }


}
