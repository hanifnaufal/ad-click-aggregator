package com.example.adaggregator.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class AttributedEvent {
    @JsonProperty("event_type")
    @Builder.Default
    private String eventType = "attributed_conversion";
    
    @JsonProperty("conversion_id")
    private String conversionId;
    
    @JsonProperty("click_id")
    private String clickId;
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("ad_id")
    private String adId;
    
    @JsonProperty("campaign_id")
    private String campaignId;
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("conversion_type")
    private String conversionType;
    
    private BigDecimal value;
    
    @JsonProperty("click_time")
    private Long clickTime;
    
    @JsonProperty("conversion_time")
    private Long conversionTime;
    
    @JsonProperty("attribution_window_hours")
    private Integer attributionWindowHours;
}
