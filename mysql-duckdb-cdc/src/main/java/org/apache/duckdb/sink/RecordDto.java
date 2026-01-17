package org.apache.duckdb.sink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.debezium.data.Envelope;
import lombok.Data;

import java.io.Serializable;

/**
 * @author suyh
 * @since 2026-01-12
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class RecordDto implements Serializable {

    private static final long serialVersionUID = 2620002976050717551L;

    /**
     * @see Envelope.Operation#code()
     */
    @JsonProperty("op")
    private String operation;

    @JsonProperty("ts_ms")
    private Long timestamp;

    @JsonProperty("before")
    private String beforeJson;

    @JsonProperty("after")
    private String afterJson;

    @JsonProperty("source")
    private CdcSourceMetaDto source;

    @JsonProperty("transaction")
    private String transaction;
}
