package com.example.adaggregator.flink;

import com.example.adaggregator.flink.model.AttributedEvent;
import com.example.adaggregator.flink.model.ClickEvent;
import com.example.adaggregator.flink.model.ConversionEvent;
import com.example.adaggregator.flink.model.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class AttributionProcessFunction extends KeyedProcessFunction<String, Event, AttributedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AttributionProcessFunction.class);
    private static final long ATTRIBUTION_WINDOW_MS = Duration.ofHours(24).toMillis();

    private transient MapState<String, ClickEvent> clicksPerCampaignState;
    private transient MapState<String, Long> processedConversionsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, ClickEvent> clickDescriptor = new MapStateDescriptor<>(
            "clicks-per-campaign",
            String.class,
            ClickEvent.class
        );
        clicksPerCampaignState = getRuntimeContext().getMapState(clickDescriptor);

        MapStateDescriptor<String, Long> conversionDescriptor = new MapStateDescriptor<>(
            "processed-conversions",
            String.class,
            Long.class
        );
        processedConversionsState = getRuntimeContext().getMapState(conversionDescriptor);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<AttributedEvent> out) throws Exception {
        if (event instanceof ClickEvent) {
            processClick((ClickEvent) event);
        } else if (event instanceof ConversionEvent) {
            processConversion((ConversionEvent) event, out);
        }
    }

    private void processClick(ClickEvent click) throws Exception {
        clicksPerCampaignState.put(click.getCampaignId(), click);
    }

    private void processConversion(ConversionEvent conversion, Collector<AttributedEvent> out) throws Exception {        
        if (processedConversionsState.contains(conversion.getEventId())) {
            LOG.info("Duplicate conversion event ignored: {}", conversion.getEventId());
            return;
        }

        ClickEvent matchingClick = clicksPerCampaignState.get(conversion.getCampaignId());

        if (matchingClick != null) {
            long timeDiff = conversion.getTimestamp() - matchingClick.getTimestamp();
            
            if (timeDiff >= 0 && timeDiff <= ATTRIBUTION_WINDOW_MS) {
                AttributedEvent attributed = AttributedEvent.builder()
                    .conversionId(conversion.getEventId())
                    .clickId(matchingClick.getEventId())
                    .userId(conversion.getUserId())
                    .adId(matchingClick.getAdId())
                    .campaignId(matchingClick.getCampaignId())
                    .source(matchingClick.getSource())
                    .conversionType(conversion.getType())
                    .value(conversion.getValue())
                    .clickTime(matchingClick.getTimestamp())
                    .conversionTime(conversion.getTimestamp())
                    .attributionWindowHours(24)
                    .build();

                out.collect(attributed);

                processedConversionsState.put(conversion.getEventId(), conversion.getTimestamp());
            } else {
                LOG.info("Conversion {} outside attribution window for campaign {}", 
                    conversion.getEventId(), conversion.getCampaignId());
            }
        } else {
            LOG.info("No matching click found for conversion {} (user: {}, campaign: {})", 
                conversion.getEventId(), conversion.getUserId(), conversion.getCampaignId());
        }
    }
}
