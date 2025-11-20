package com.example.adaggregator.flink;

import com.example.adaggregator.flink.model.AttributedEvent;
import com.example.adaggregator.flink.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.example.adaggregator.flink.serialization.EventDeserializationSchema;
import com.example.adaggregator.flink.serialization.AttributedEventSerializationSchema;

public class AttributionJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = "raw-events";
        String outputTopic = "attributed-events";

        // 1. Source: Read from Kafka
        KafkaSource<Event> source = KafkaSource.<Event>builder()
            .setBootstrapServers(kafkaBootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("flink-attribution-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new EventDeserializationSchema())
            .build();

        DataStream<Event> events = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2. Process: KeyBy User -> Attribute
        DataStream<AttributedEvent> attributedEvents = events
            .keyBy(Event::getUserId)
            .process(new AttributionProcessFunction())
            .name("Attribution Logic");

        // 3. Sink: Write to Kafka
        KafkaSink<AttributedEvent> sink = KafkaSink.<AttributedEvent>builder()
            .setBootstrapServers(kafkaBootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new AttributedEventSerializationSchema())
                .build()
            )
            .build();

        attributedEvents.sinkTo(sink).name("Kafka Sink");

        env.execute("Ad Click Attribution Job");
    }
}
