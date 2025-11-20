package com.example.adaggregator.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

@Data
@EqualsAndHashCode(callSuper = true)
public class ClickEvent extends Event {
    @NotNull
    @JsonProperty("ad_id")
    private String adId;
    
    @NotNull
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
}
