package com.example.adaggregator.flink;

import com.example.adaggregator.flink.model.AttributedEvent;
import com.example.adaggregator.flink.model.ClickEvent;
import com.example.adaggregator.flink.model.ConversionEvent;
import com.example.adaggregator.flink.model.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AttributionProcessFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Event, AttributedEvent> testHarness;
    private static final long ATTRIBUTION_WINDOW_MS = Duration.ofHours(24).toMillis();

    @BeforeEach
    void setUp() throws Exception {
        AttributionProcessFunction function = new AttributionProcessFunction();
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(function),
            Event::getUserId,
            TypeInformation.of(String.class)
        );
        testHarness.open();
    }

    @Test
    void testClickEventIsStoredInState() throws Exception {
        // Given: A click event
        ClickEvent click = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 1000L);

        // When: Processing the click
        testHarness.processElement(click, 1000L);

        // Then: No output is produced (clicks are just stored)
        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    void testConversionAttributedToClick() throws Exception {
        // Given: A click event followed by a conversion within attribution window
        ClickEvent click = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 1000L);
        ConversionEvent conversion = createConversionEvent("conv-1", "user-1", "campaign-1", 
            5000L, "purchase", new BigDecimal("99.99"));

        // When: Processing both events
        testHarness.processElement(click, 1000L);
        testHarness.processElement(conversion, 5000L);

        // Then: An attributed event is produced
        List<AttributedEvent> output = testHarness.extractOutputValues();
        assertThat(output).hasSize(1);

        AttributedEvent attributed = output.get(0);
        assertThat(attributed.getConversionId()).isEqualTo("conv-1");
        assertThat(attributed.getClickId()).isEqualTo("click-1");
        assertThat(attributed.getUserId()).isEqualTo("user-1");
        assertThat(attributed.getAdId()).isEqualTo("ad-1");
        assertThat(attributed.getCampaignId()).isEqualTo("campaign-1");
        assertThat(attributed.getSource()).isEqualTo("google");
        assertThat(attributed.getConversionType()).isEqualTo("purchase");
        assertThat(attributed.getValue()).isEqualTo(new BigDecimal("99.99"));
        assertThat(attributed.getClickTime()).isEqualTo(1000L);
        assertThat(attributed.getConversionTime()).isEqualTo(5000L);
        assertThat(attributed.getAttributionWindowHours()).isEqualTo(24);
    }

    @Test
    void testConversionOutsideAttributionWindow() throws Exception {
        // Given: A click and a conversion outside the 24-hour window
        ClickEvent click = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 1000L);
        long conversionTime = 1000L + ATTRIBUTION_WINDOW_MS + 1000L; // 1 second past window
        ConversionEvent conversion = createConversionEvent("conv-1", "user-1", "campaign-1", 
            conversionTime, "purchase", new BigDecimal("99.99"));

        // When: Processing both events
        testHarness.processElement(click, 1000L);
        testHarness.processElement(conversion, conversionTime);

        // Then: No attributed event is produced
        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    void testConversionBeforeClickNotAttributed() throws Exception {
        // Given: A conversion that happens before the click
        ClickEvent click = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 5000L);
        ConversionEvent conversion = createConversionEvent("conv-1", "user-1", "campaign-1", 
            1000L, "purchase", new BigDecimal("99.99"));

        // When: Processing both events
        testHarness.processElement(click, 5000L);
        testHarness.processElement(conversion, 1000L);

        // Then: No attributed event is produced (negative time diff)
        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    void testDuplicateConversionIgnored() throws Exception {
        // Given: A click and two identical conversions
        ClickEvent click = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 1000L);
        ConversionEvent conversion1 = createConversionEvent("conv-1", "user-1", "campaign-1", 
            5000L, "purchase", new BigDecimal("99.99"));
        ConversionEvent conversion2 = createConversionEvent("conv-1", "user-1", "campaign-1", 
            6000L, "purchase", new BigDecimal("99.99"));

        // When: Processing click and both conversions
        testHarness.processElement(click, 1000L);
        testHarness.processElement(conversion1, 5000L);
        testHarness.processElement(conversion2, 6000L);

        // Then: Only one attributed event is produced
        List<AttributedEvent> output = testHarness.extractOutputValues();
        assertThat(output).hasSize(1);
        assertThat(output.get(0).getConversionId()).isEqualTo("conv-1");
    }

    @Test
    void testConversionWithoutMatchingClick() throws Exception {
        // Given: A conversion without a prior click for that campaign
        ConversionEvent conversion = createConversionEvent("conv-1", "user-1", "campaign-1", 
            5000L, "purchase", new BigDecimal("99.99"));

        // When: Processing the conversion
        testHarness.processElement(conversion, 5000L);

        // Then: No attributed event is produced
        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    void testMultipleCampaignsPerUser() throws Exception {
        // Given: Clicks and conversions for different campaigns for the same user
        ClickEvent click1 = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 1000L);
        ClickEvent click2 = createClickEvent("click-2", "user-1", "campaign-2", "ad-2", 2000L);
        ConversionEvent conversion1 = createConversionEvent("conv-1", "user-1", "campaign-1", 
            3000L, "purchase", new BigDecimal("50.00"));
        ConversionEvent conversion2 = createConversionEvent("conv-2", "user-1", "campaign-2", 
            4000L, "signup", new BigDecimal("0.00"));

        // When: Processing all events
        testHarness.processElement(click1, 1000L);
        testHarness.processElement(click2, 2000L);
        testHarness.processElement(conversion1, 3000L);
        testHarness.processElement(conversion2, 4000L);

        // Then: Two attributed events are produced, each matching the correct campaign
        List<AttributedEvent> output = testHarness.extractOutputValues();
        assertThat(output).hasSize(2);

        AttributedEvent attr1 = output.stream()
            .filter(e -> e.getCampaignId().equals("campaign-1"))
            .findFirst()
            .orElseThrow();
        assertThat(attr1.getClickId()).isEqualTo("click-1");
        assertThat(attr1.getConversionId()).isEqualTo("conv-1");

        AttributedEvent attr2 = output.stream()
            .filter(e -> e.getCampaignId().equals("campaign-2"))
            .findFirst()
            .orElseThrow();
        assertThat(attr2.getClickId()).isEqualTo("click-2");
        assertThat(attr2.getConversionId()).isEqualTo("conv-2");
    }

    @Test
    void testConversionAtExactWindowBoundary() throws Exception {
        // Given: A click and conversion exactly at the 24-hour boundary
        ClickEvent click = createClickEvent("click-1", "user-1", "campaign-1", "ad-1", 1000L);
        long conversionTime = 1000L + ATTRIBUTION_WINDOW_MS; // Exactly at boundary
        ConversionEvent conversion = createConversionEvent("conv-1", "user-1", "campaign-1", 
            conversionTime, "purchase", new BigDecimal("99.99"));

        // When: Processing both events
        testHarness.processElement(click, 1000L);
        testHarness.processElement(conversion, conversionTime);

        // Then: Attribution should succeed (boundary is inclusive)
        List<AttributedEvent> output = testHarness.extractOutputValues();
        assertThat(output).hasSize(1);
    }

    // Helper methods to create test events
    private ClickEvent createClickEvent(String eventId, String userId, String campaignId, 
                                       String adId, long timestamp) {
        ClickEvent click = new ClickEvent();
        click.setEventType("click");
        click.setEventId(eventId);
        click.setUserId(userId);
        click.setCampaignId(campaignId);
        click.setAdId(adId);
        click.setSource("google");
        click.setTimestamp(timestamp);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("device", "mobile");
        click.setMetadata(metadata);
        
        return click;
    }

    private ConversionEvent createConversionEvent(String eventId, String userId, String campaignId,
                                                  long timestamp, String type, BigDecimal value) {
        ConversionEvent conversion = new ConversionEvent();
        conversion.setEventType("conversion");
        conversion.setEventId(eventId);
        conversion.setUserId(userId);
        conversion.setCampaignId(campaignId);
        conversion.setTimestamp(timestamp);
        conversion.setType(type);
        conversion.setValue(value);
        conversion.setSource("app");
        
        return conversion;
    }
}
