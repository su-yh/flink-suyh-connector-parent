package org.apache.duckdb.sink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.debezium.data.Envelope;
import lombok.Data;

import java.util.Map;

/**
 * @author suyh
 * @since 2026-01-12
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class RecordDto {
    /**
     * @see Envelope.Operation#code()
     */
    @JsonProperty("op")
    private String operation;

    @JsonProperty("ts_ms")
    private Long timestamp;

    @JsonProperty("before")
    private Map<String, Object> before;

    @JsonProperty("after")
    private Map<String, Object> after;

    @JsonProperty("source")
    private CdcSourceMetaDto source;

    @JsonProperty("transaction")
    private Map<String, Object> transaction;
}
