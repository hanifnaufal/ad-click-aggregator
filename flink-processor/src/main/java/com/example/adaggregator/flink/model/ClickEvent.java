package com.example.adaggregator.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class ClickEvent extends Event {
    @JsonProperty("ad_id")
    private String adId;
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
}
