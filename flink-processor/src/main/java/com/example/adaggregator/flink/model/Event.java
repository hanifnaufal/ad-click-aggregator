package com.example.adaggregator.flink.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "event_type",
    visible = true
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ClickEvent.class, name = "click"),
    @JsonSubTypes.Type(value = ConversionEvent.class, name = "conversion")
})
public abstract class Event {
    @JsonProperty("event_type")
    private String eventType;
    
    @JsonProperty("event_id")
    private String eventId;
    
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("campaign_id")
    private String campaignId;
    
    private Long timestamp;
}
