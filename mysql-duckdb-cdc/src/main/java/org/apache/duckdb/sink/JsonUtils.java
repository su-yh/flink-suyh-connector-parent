package org.apache.duckdb.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.MapType;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@Slf4j
public class JsonUtils {
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = JsonMapper.builder()
                .defaultTimeZone(TimeZone.getDefault())
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                .build();
    }

    /**
     * 序列化对象
     *
     * @param object 目标对象
     * @return 返回json 字符串
     */
    public static String serializable(Object object) {
        return serializable(object, OBJECT_MAPPER);
    }

    public static String serializable(Object object, ObjectMapper mapper) {
        String res = null;
        try {
            res = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("serializable object failed. object: " + object, e);
        }

        return res;
    }

    /**
     * 反序列化对象
     *
     * @param json  json 字符串
     * @param clazz 解析的对象类型
     * @param <T>   对象类型
     * @return 返回对象实体
     */
    public static <T> T deserialize(String json, Class<T> clazz) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, clazz);
    }

    /**
     * 数组反序列化
     *
     * @param json  json 格式字符串
     * @param clazz 泛型类型
     * @param <T>   泛型
     * @return 返回List 对象
     */
    public static <T> T[] deserializeToArray(String json, Class<T> clazz) {
       return deserializeToArray(json, clazz, OBJECT_MAPPER);
    }

    public static <T> T[] deserializeToArray(String json, Class<T> clazz, ObjectMapper mapper) {
        try {
            ArrayType arrayType = mapper.getTypeFactory().constructArrayType(clazz);
            return mapper.readValue(json, arrayType);
        } catch (JsonProcessingException e) {
            log.error("deserializeList object failed. json string: " + json, e);
        }

        return null;
    }

    public static <T> List<T> deserializeToList(String json, Class<T> clazz) {
        if (StringUtils.isNullOrWhitespaceOnly(json)) {
            return Collections.emptyList();
        } else {
            return deserializeToList(json, clazz, OBJECT_MAPPER);
        }
    }

    public static <T> List<T> deserializeToList(String json, Class<T> clazz, ObjectMapper mapper) {
        try {
            JavaType javaType = mapper.getTypeFactory()
                    .constructParametricType(List.class, clazz);
            return mapper.readValue(json, javaType);
        } catch (JsonProcessingException e) {
            log.error("deserializeToList02 failed. json string: {}", json, e);
        }

        return null;
    }

    /**
     * 反序列化 Map
     *
     * @param json   json 格式字符串
     * @param kClazz map-key
     * @param vClass map-value
     * @param <K>    map-key class
     * @param <V>    map-value class
     * @return 返回map
     */
    public static <K, V> Map<K, V> deserializeToMap(String json, Class<K> kClazz, Class<V> vClass) {
        return deserializeToMap(json, kClazz, vClass, OBJECT_MAPPER);
    }

    public static <K, V> Map<K, V> deserializeToMap(String json, Class<K> kClazz, Class<V> vClass, ObjectMapper mapper) {
        try {
            MapType mapType = mapper.getTypeFactory().constructMapType(Map.class, kClazz, vClass);
            return mapper.readValue(json, mapType);
        } catch (JsonProcessingException e) {
            log.error("deserializeMap object failed. json string: " + json, e);
        }

        return null;
    }

    public static ObjectNode createObjectNode() {
        return OBJECT_MAPPER.createObjectNode();
    }

    public static ArrayNode createArrayNode() {
        return OBJECT_MAPPER.createArrayNode();
    }

    /**
     * 将一个语法正确的json 字符串转换成一个JsonNode 对象
     *
     * @param json 语法正确的json 字符串
     * @return 返回一个JsonNode 对象
     */
    public static JsonNode deserializeToJsonNode(String json) throws JsonProcessingException {
        return deserializeToJsonNode(json, OBJECT_MAPPER);
    }

    public static JsonNode deserializeToJsonNode(String json, ObjectMapper mapper) throws JsonProcessingException {
        return mapper.readTree(json);
    }

    /**
     * 将一个json 数组字符串转换成一个ArrayNode 对象
     * <p>
     * JsonNode 可以是数组，如果确定是数组则可以直接强制类型转换
     *
     * @param json 语法正确的Json 数组字符串
     * @return 返回一个ArrayNode 对象
     */
    public static ArrayNode deserializeToArrayNode(String json) throws JsonProcessingException {
        return deserializeToArrayNode(json, OBJECT_MAPPER);
    }

    public static ArrayNode deserializeToArrayNode(String json, ObjectMapper mapper) throws JsonProcessingException {
        return (ArrayNode) deserializeToJsonNode(json);
    }

    /**
     * 校验JSON字符串是否合法
     */
    public static boolean isValidJson(String json) {
        try {
            if (StringUtils.isNullOrWhitespaceOnly(json)) {
                log.warn("待校验的JSON字符串为空或空白");
                return false;
            }
            JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
            return jsonNode != null;
        } catch (JsonProcessingException e) {
            return false;
        }
    }
}