package com.example.adaggregator.service;

import com.example.adaggregator.model.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventProducer {

    private final KafkaSender<String, Event> kafkaSender;
    private static final String TOPIC = "raw-events";

    public Mono<Void> sendEvent(Event event) {
        return kafkaSender.send(
            Mono.just(
                SenderRecord.create(
                    new ProducerRecord<>(
                        TOPIC,
                        event.getUserId(),
                        event
                    ), event.getEventId()
                )
            )
        )
        .doOnError(e -> log.error("Send failed", e))
        .then();
    }
}
