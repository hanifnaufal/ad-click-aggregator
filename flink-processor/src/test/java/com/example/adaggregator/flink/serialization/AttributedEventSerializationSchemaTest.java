package com.example.adaggregator.flink.serialization;

import com.example.adaggregator.flink.model.AttributedEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class AttributedEventSerializationSchemaTest {

    private AttributedEventSerializationSchema schema;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        schema = new AttributedEventSerializationSchema();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testSerializeAttributedEvent() throws IOException {
        // Given: An attributed event
        AttributedEvent event = AttributedEvent.builder()
            .conversionId("conv-123")
            .clickId("click-456")
            .userId("user-789")
            .adId("ad-001")
            .campaignId("campaign-002")
            .source("google")
            .conversionType("purchase")
            .value(new BigDecimal("99.99"))
            .clickTime(1000000L)
            .conversionTime(2000000L)
            .attributionWindowHours(24)
            .build();

        // When: Serializing the event
        byte[] serialized = schema.serialize(event);

        // Then: The JSON contains all expected fields
        JsonNode json = objectMapper.readTree(serialized);
        
        assertThat(json.get("event_type").asText()).isEqualTo("attributed_conversion");
        assertThat(json.get("conversion_id").asText()).isEqualTo("conv-123");
        assertThat(json.get("click_id").asText()).isEqualTo("click-456");
        assertThat(json.get("user_id").asText()).isEqualTo("user-789");
        assertThat(json.get("ad_id").asText()).isEqualTo("ad-001");
        assertThat(json.get("campaign_id").asText()).isEqualTo("campaign-002");
        assertThat(json.get("source").asText()).isEqualTo("google");
        assertThat(json.get("conversion_type").asText()).isEqualTo("purchase");
        assertThat(json.get("value").asText()).isEqualTo("99.99");
        assertThat(json.get("click_time").asLong()).isEqualTo(1000000L);
        assertThat(json.get("conversion_time").asLong()).isEqualTo(2000000L);
        assertThat(json.get("attribution_window_hours").asInt()).isEqualTo(24);
    }

    @Test
    void testSerializeWithDefaultEventType() throws IOException {
        // Given: An attributed event without explicitly setting eventType
        AttributedEvent event = AttributedEvent.builder()
            .conversionId("conv-123")
            .clickId("click-456")
            .userId("user-789")
            .adId("ad-001")
            .campaignId("campaign-002")
            .source("facebook")
            .conversionType("signup")
            .value(BigDecimal.ZERO)
            .clickTime(1000000L)
            .conversionTime(2000000L)
            .attributionWindowHours(24)
            .build();

        // When: Serializing the event
        byte[] serialized = schema.serialize(event);

        // Then: The default event_type is present
        JsonNode json = objectMapper.readTree(serialized);
        assertThat(json.get("event_type").asText()).isEqualTo("attributed_conversion");
    }

    @Test
    void testSerializeWithZeroValue() throws IOException {
        // Given: An attributed event with zero value (e.g., signup conversion)
        AttributedEvent event = AttributedEvent.builder()
            .conversionId("conv-123")
            .clickId("click-456")
            .userId("user-789")
            .adId("ad-001")
            .campaignId("campaign-002")
            .source("twitter")
            .conversionType("signup")
            .value(BigDecimal.ZERO)
            .clickTime(1000000L)
            .conversionTime(2000000L)
            .attributionWindowHours(24)
            .build();

        // When: Serializing the event
        byte[] serialized = schema.serialize(event);

        // Then: Value is serialized as 0
        JsonNode json = objectMapper.readTree(serialized);
        assertThat(json.get("value").asText()).isEqualTo("0");
    }

    @Test
    void testSerializeWithLargeValue() throws IOException {
        // Given: An attributed event with a large monetary value
        AttributedEvent event = AttributedEvent.builder()
            .conversionId("conv-123")
            .clickId("click-456")
            .userId("user-789")
            .adId("ad-001")
            .campaignId("campaign-002")
            .source("google")
            .conversionType("purchase")
            .value(new BigDecimal("9999999.99"))
            .clickTime(1000000L)
            .conversionTime(2000000L)
            .attributionWindowHours(24)
            .build();

        // When: Serializing the event
        byte[] serialized = schema.serialize(event);

        // Then: Large value is preserved
        JsonNode json = objectMapper.readTree(serialized);
        assertThat(json.get("value").asText()).isEqualTo("9999999.99");
    }

    @Test
    void testSerializeProducesValidJson() throws IOException {
        // Given: An attributed event
        AttributedEvent event = AttributedEvent.builder()
            .conversionId("conv-123")
            .clickId("click-456")
            .userId("user-789")
            .adId("ad-001")
            .campaignId("campaign-002")
            .source("google")
            .conversionType("purchase")
            .value(new BigDecimal("99.99"))
            .clickTime(1000000L)
            .conversionTime(2000000L)
            .attributionWindowHours(24)
            .build();

        // When: Serializing the event
        byte[] serialized = schema.serialize(event);

        // Then: The output is valid JSON that can be parsed
        JsonNode json = objectMapper.readTree(serialized);
        assertThat(json).isNotNull();
        assertThat(json.isObject()).isTrue();
    }

    @Test
    void testSerializeMultipleEvents() throws IOException {
        // Given: Multiple different attributed events
        AttributedEvent event1 = AttributedEvent.builder()
            .conversionId("conv-1")
            .clickId("click-1")
            .userId("user-1")
            .adId("ad-1")
            .campaignId("campaign-1")
            .source("google")
            .conversionType("purchase")
            .value(new BigDecimal("50.00"))
            .clickTime(1000L)
            .conversionTime(2000L)
            .attributionWindowHours(24)
            .build();

        AttributedEvent event2 = AttributedEvent.builder()
            .conversionId("conv-2")
            .clickId("click-2")
            .userId("user-2")
            .adId("ad-2")
            .campaignId("campaign-2")
            .source("facebook")
            .conversionType("signup")
            .value(BigDecimal.ZERO)
            .clickTime(3000L)
            .conversionTime(4000L)
            .attributionWindowHours(24)
            .build();

        // When: Serializing both events
        byte[] serialized1 = schema.serialize(event1);
        byte[] serialized2 = schema.serialize(event2);

        // Then: Both produce valid, distinct JSON
        JsonNode json1 = objectMapper.readTree(serialized1);
        JsonNode json2 = objectMapper.readTree(serialized2);
        
        assertThat(json1.get("conversion_id").asText()).isEqualTo("conv-1");
        assertThat(json2.get("conversion_id").asText()).isEqualTo("conv-2");
        assertThat(json1).isNotEqualTo(json2);
    }
}
