package com.example.adaggregator.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

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
    @NotNull
    @JsonProperty("event_type")
    private String eventType;
    
    @NotNull
    @JsonProperty("event_id")
    private String eventId;
    
    @NotNull
    @JsonProperty("user_id")
    private String userId;
    
    @NotNull
    @Positive
    private Long timestamp;
}
