package com.example.adaggregator.controller;

import com.example.adaggregator.model.Event;
import com.example.adaggregator.service.EventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class IngestionController {

    private final EventProducer eventProducer;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> ingestEvent(@RequestBody @Valid Event event) {
        return eventProducer.sendEvent(event);
    }
}
