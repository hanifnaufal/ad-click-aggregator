package com.example.adaggregator.flink.serialization;

import com.example.adaggregator.flink.model.AttributedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class AttributedEventSerializationSchema implements SerializationSchema<AttributedEvent> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(AttributedEvent element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize AttributedEvent", e);
        }
    }
}
