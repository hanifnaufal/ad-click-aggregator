package com.example.adaggregator.flink.serialization;

import com.example.adaggregator.flink.model.ClickEvent;
import com.example.adaggregator.flink.model.ConversionEvent;
import com.example.adaggregator.flink.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventDeserializationSchemaTest {

    private EventDeserializationSchema schema;

    @BeforeEach
    void setUp() {
        schema = new EventDeserializationSchema();
    }

    @Test
    void testDeserializeClickEvent() throws IOException {
        // Given: A JSON click event
        String clickJson = """
            {
                "event_type": "click",
                "event_id": "click-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "ad_id": "ad-001",
                "source": "google",
                "metadata": {
                    "device": "mobile",
                    "browser": "chrome"
                }
            }
            """;

        // When: Deserializing the JSON
        Event event = schema.deserialize(clickJson.getBytes());

        // Then: A ClickEvent is created with correct fields
        assertThat(event).isInstanceOf(ClickEvent.class);
        ClickEvent click = (ClickEvent) event;
        
        assertThat(click.getEventType()).isEqualTo("click");
        assertThat(click.getEventId()).isEqualTo("click-123");
        assertThat(click.getUserId()).isEqualTo("user-456");
        assertThat(click.getCampaignId()).isEqualTo("campaign-789");
        assertThat(click.getTimestamp()).isEqualTo(1234567890L);
        assertThat(click.getAdId()).isEqualTo("ad-001");
        assertThat(click.getSource()).isEqualTo("google");
        assertThat(click.getMetadata()).containsEntry("device", "mobile");
        assertThat(click.getMetadata()).containsEntry("browser", "chrome");
    }

    @Test
    void testDeserializeConversionEvent() throws IOException {
        // Given: A JSON conversion event
        String conversionJson = """
            {
                "event_type": "conversion",
                "event_id": "conv-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "type": "purchase",
                "value": 99.99,
                "source": "app"
            }
            """;

        // When: Deserializing the JSON
        Event event = schema.deserialize(conversionJson.getBytes());

        // Then: A ConversionEvent is created with correct fields
        assertThat(event).isInstanceOf(ConversionEvent.class);
        ConversionEvent conversion = (ConversionEvent) event;
        
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
    void testDeserializeClickEventWithMinimalFields() throws IOException {
        // Given: A click event with only required fields
        String clickJson = """
            {
                "event_type": "click",
                "event_id": "click-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890,
                "ad_id": "ad-001",
                "source": "facebook"
            }
            """;

        // When: Deserializing the JSON
        Event event = schema.deserialize(clickJson.getBytes());

        // Then: A ClickEvent is created
        assertThat(event).isInstanceOf(ClickEvent.class);
        ClickEvent click = (ClickEvent) event;
        assertThat(click.getEventId()).isEqualTo("click-123");
        assertThat(click.getMetadata()).isNull();
    }

    @Test
    void testDeserializeInvalidJson() {
        // Given: Invalid JSON
        String invalidJson = "{ invalid json }";

        // When/Then: Deserialization throws an exception
        assertThatThrownBy(() -> schema.deserialize(invalidJson.getBytes()))
            .isInstanceOf(IOException.class);
    }

    @Test
    void testDeserializeMissingEventType() {
        // Given: JSON without event_type field
        String jsonWithoutType = """
            {
                "event_id": "click-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890
            }
            """;

        // When/Then: Deserialization throws an exception
        assertThatThrownBy(() -> schema.deserialize(jsonWithoutType.getBytes()))
            .isInstanceOf(IOException.class);
    }

    @Test
    void testDeserializeUnknownEventType() {
        // Given: JSON with unknown event_type
        String unknownTypeJson = """
            {
                "event_type": "unknown",
                "event_id": "evt-123",
                "user_id": "user-456",
                "campaign_id": "campaign-789",
                "timestamp": 1234567890
            }
            """;

        // When/Then: Deserialization throws an exception
        assertThatThrownBy(() -> schema.deserialize(unknownTypeJson.getBytes()))
            .isInstanceOf(IOException.class);
    }

    @Test
    void testIsEndOfStream() throws IOException {
        // Given: Any event
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
        Event event = schema.deserialize(clickJson.getBytes());

        // When/Then: isEndOfStream always returns false
        assertThat(schema.isEndOfStream(event)).isFalse();
    }

    @Test
    void testGetProducedType() {
        // When/Then: getProducedType returns Event.class type information
        assertThat(schema.getProducedType().getTypeClass()).isEqualTo(Event.class);
    }
}
