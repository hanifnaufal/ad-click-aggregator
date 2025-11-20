package com.example.adaggregator.flink.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ModelTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void testClickEventCreation() {
        // Given/When: Creating a ClickEvent
        ClickEvent click = new ClickEvent();
        click.setEventType("click");
        click.setEventId("click-123");
        click.setUserId("user-456");
        click.setCampaignId("campaign-789");
        click.setTimestamp(1234567890L);
        click.setAdId("ad-001");
        click.setSource("google");
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("device", "mobile");
        click.setMetadata(metadata);

        // Then: All fields are set correctly
        assertThat(click.getEventType()).isEqualTo("click");
        assertThat(click.getEventId()).isEqualTo("click-123");
        assertThat(click.getUserId()).isEqualTo("user-456");
        assertThat(click.getCampaignId()).isEqualTo("campaign-789");
        assertThat(click.getTimestamp()).isEqualTo(1234567890L);
        assertThat(click.getAdId()).isEqualTo("ad-001");
        assertThat(click.getSource()).isEqualTo("google");
        assertThat(click.getMetadata()).containsEntry("device", "mobile");
    }

    @Test
    void testConversionEventCreation() {
        // Given/When: Creating a ConversionEvent
        ConversionEvent conversion = new ConversionEvent();
        conversion.setEventType("conversion");
        conversion.setEventId("conv-123");
        conversion.setUserId("user-456");
        conversion.setCampaignId("campaign-789");
        conversion.setTimestamp(1234567890L);
        conversion.setType("purchase");
        conversion.setValue(new BigDecimal("99.99"));
        conversion.setSource("app");

        // Then: All fields are set correctly
        assertThat(conversion.getEventType()).isEqualTo("conversion");
        assertThat(conversion.getEventId()).isEqualTo("conv-123");
        assertThat(conversion.getUserId()).isEqualTo("user-456");
        assertThat(conversion.getCampaignId()).isEqualTo("campaign-789");
        assertThat(conversion.getTimestamp()).isEqualTo(1234567890L);
        assertThat(conversion.getType()).isEqualTo("purchase");
        assertThat(conversion.getValue()).isEqualByComparingTo(new BigDecimal("99.99"));
        assertThat(conversion.getSource()).isEqualTo("app");
    }

    @Test
    void testAttributedEventBuilder() {
        // Given/When: Building an AttributedEvent
        AttributedEvent attributed = AttributedEvent.builder()
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

        // Then: All fields are set correctly
        assertThat(attributed.getEventType()).isEqualTo("attributed_conversion");
        assertThat(attributed.getConversionId()).isEqualTo("conv-123");
        assertThat(attributed.getClickId()).isEqualTo("click-456");
        assertThat(attributed.getUserId()).isEqualTo("user-789");
        assertThat(attributed.getAdId()).isEqualTo("ad-001");
        assertThat(attributed.getCampaignId()).isEqualTo("campaign-002");
        assertThat(attributed.getSource()).isEqualTo("google");
        assertThat(attributed.getConversionType()).isEqualTo("purchase");
        assertThat(attributed.getValue()).isEqualByComparingTo(new BigDecimal("99.99"));
        assertThat(attributed.getClickTime()).isEqualTo(1000000L);
        assertThat(attributed.getConversionTime()).isEqualTo(2000000L);
        assertThat(attributed.getAttributionWindowHours()).isEqualTo(24);
    }

    @Test
    void testClickEventJsonSerialization() throws IOException {
        // Given: A ClickEvent
        ClickEvent click = new ClickEvent();
        click.setEventType("click");
        click.setEventId("click-123");
        click.setUserId("user-456");
        click.setCampaignId("campaign-789");
        click.setTimestamp(1234567890L);
        click.setAdId("ad-001");
        click.setSource("google");

        // When: Serializing to JSON
        String json = objectMapper.writeValueAsString(click);

        // Then: JSON contains correct field names with underscores
        assertThat(json).contains("\"event_type\":\"click\"");
        assertThat(json).contains("\"event_id\":\"click-123\"");
        assertThat(json).contains("\"user_id\":\"user-456\"");
        assertThat(json).contains("\"campaign_id\":\"campaign-789\"");
        assertThat(json).contains("\"ad_id\":\"ad-001\"");
    }

    @Test
    void testConversionEventJsonSerialization() throws IOException {
        // Given: A ConversionEvent
        ConversionEvent conversion = new ConversionEvent();
        conversion.setEventType("conversion");
        conversion.setEventId("conv-123");
        conversion.setUserId("user-456");
        conversion.setCampaignId("campaign-789");
        conversion.setTimestamp(1234567890L);
        conversion.setType("purchase");
        conversion.setValue(new BigDecimal("99.99"));

        // When: Serializing to JSON
        String json = objectMapper.writeValueAsString(conversion);

        // Then: JSON contains correct field names
        assertThat(json).contains("\"event_type\":\"conversion\"");
        assertThat(json).contains("\"event_id\":\"conv-123\"");
        assertThat(json).contains("\"user_id\":\"user-456\"");
        assertThat(json).contains("\"campaign_id\":\"campaign-789\"");
    }

    @Test
    void testClickEventJsonDeserialization() throws IOException {
        // Given: JSON for a click event
        String json = """
            {
                "event_type": "click",
                "event_id": "click-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "ad_id": "ad-001",
                "source": "google"
            }
            """;

        // When: Deserializing from JSON
        Event event = objectMapper.readValue(json, Event.class);

        // Then: A ClickEvent is created
        assertThat(event).isInstanceOf(ClickEvent.class);
        ClickEvent click = (ClickEvent) event;
        assertThat(click.getEventId()).isEqualTo("click-123");
        assertThat(click.getAdId()).isEqualTo("ad-001");
    }

    @Test
    void testConversionEventJsonDeserialization() throws IOException {
        // Given: JSON for a conversion event
        String json = """
            {
                "event_type": "conversion",
                "event_id": "conv-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "type": "purchase",
                "value": 99.99
            }
            """;

        // When: Deserializing from JSON
        Event event = objectMapper.readValue(json, Event.class);

        // Then: A ConversionEvent is created
        assertThat(event).isInstanceOf(ConversionEvent.class);
        ConversionEvent conversion = (ConversionEvent) event;
        assertThat(conversion.getEventId()).isEqualTo("conv-123");
        assertThat(conversion.getType()).isEqualTo("purchase");
        assertThat(conversion.getValue()).isEqualByComparingTo(new BigDecimal("99.99"));
    }

    @Test
    void testPolymorphicDeserialization() throws IOException {
        // Given: JSON for both click and conversion events
        String clickJson = """
            {
                "event_type": "click",
                "event_id": "click-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "ad_id": "ad-001",
                "source": "google"
            }
            """;
        
        String conversionJson = """
            {
                "event_type": "conversion",
                "event_id": "conv-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "type": "purchase",
                "value": 99.99
            }
            """;

        // When: Deserializing both
        Event clickEvent = objectMapper.readValue(clickJson, Event.class);
        Event conversionEvent = objectMapper.readValue(conversionJson, Event.class);

        // Then: Correct subclasses are created
        assertThat(clickEvent).isInstanceOf(ClickEvent.class);
        assertThat(conversionEvent).isInstanceOf(ConversionEvent.class);
    }

    @Test
    void testClickEventEquality() {
        // Given: Two identical ClickEvents
        ClickEvent click1 = new ClickEvent();
        click1.setEventId("click-123");
        click1.setUserId("user-456");
        click1.setAdId("ad-001");

        ClickEvent click2 = new ClickEvent();
        click2.setEventId("click-123");
        click2.setUserId("user-456");
        click2.setAdId("ad-001");

        // Then: They are equal
        assertThat(click1).isEqualTo(click2);
        assertThat(click1.hashCode()).isEqualTo(click2.hashCode());
    }

    @Test
    void testConversionEventEquality() {
        // Given: Two identical ConversionEvents
        ConversionEvent conv1 = new ConversionEvent();
        conv1.setEventId("conv-123");
        conv1.setUserId("user-456");
        conv1.setType("purchase");
        conv1.setValue(new BigDecimal("99.99"));

        ConversionEvent conv2 = new ConversionEvent();
        conv2.setEventId("conv-123");
        conv2.setUserId("user-456");
        conv2.setType("purchase");
        conv2.setValue(new BigDecimal("99.99"));

        // Then: They are equal
        assertThat(conv1).isEqualTo(conv2);
        assertThat(conv1.hashCode()).isEqualTo(conv2.hashCode());
    }
}
