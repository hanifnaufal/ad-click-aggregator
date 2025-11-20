package com.example.adaggregator.flink;

import com.example.adaggregator.flink.model.AttributedEvent;
import com.example.adaggregator.flink.model.ClickEvent;
import com.example.adaggregator.flink.model.ConversionEvent;
import com.example.adaggregator.flink.model.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class AttributionProcessFunction extends KeyedProcessFunction<String, Event, AttributedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AttributionProcessFunction.class);
    private static final long ATTRIBUTION_WINDOW_MS = Duration.ofHours(24).toMillis();

    private transient ValueState<ClickEvent> lastClickState;
    private transient MapState<String, Long> processedConversionsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<ClickEvent> clickDescriptor = new ValueStateDescriptor<>(
            "last-click",
            ClickEvent.class
        );
        lastClickState = getRuntimeContext().getState(clickDescriptor);

        MapStateDescriptor<String, Long> conversionDescriptor = new MapStateDescriptor<>(
            "processed-conversions",
            String.class,
            Long.class
        );
        // In a real production scenario, we should configure State TTL for this MapState to avoid infinite growth.
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
        lastClickState.update(click);
    }

    private void processConversion(ConversionEvent conversion, Collector<AttributedEvent> out) throws Exception {        
        if (processedConversionsState.contains(conversion.getEventId())) {
            LOG.info("Duplicate conversion event ignored: {}", conversion.getEventId());
            return;
        }

        ClickEvent lastClick = lastClickState.value();

        if (lastClick != null) {
            long timeDiff = conversion.getTimestamp() - lastClick.getTimestamp();
            
            if (timeDiff >= 0 && timeDiff <= ATTRIBUTION_WINDOW_MS) {
                AttributedEvent attributed = AttributedEvent.builder()
                    .conversionId(conversion.getEventId())
                    .clickId(lastClick.getEventId())
                    .userId(conversion.getUserId())
                    .adId(lastClick.getAdId())
                    .campaignId(lastClick.getCampaignId())
                    .source(lastClick.getSource())
                    .conversionType(conversion.getType())
                    .value(conversion.getValue())
                    .clickTime(lastClick.getTimestamp())
                    .conversionTime(conversion.getTimestamp())
                    .attributionWindowHours(24)
                    .build();

                out.collect(attributed);

                processedConversionsState.put(conversion.getEventId(), conversion.getTimestamp());
            } else {
                LOG.info("Conversion {} outside attribution window for user {}", conversion.getEventId(), conversion.getUserId());
            }
        } else {
            LOG.info("No matching click found for conversion {} (user {})", conversion.getEventId(), conversion.getUserId());
        }
    }
}
