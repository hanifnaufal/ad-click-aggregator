package com.example.adaggregator.controller;

import com.example.adaggregator.model.ClickEvent;
import com.example.adaggregator.service.EventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@WebFluxTest(IngestionController.class)
class IngestionControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private EventProducer eventProducer;

    @Test
    void ingestEvent_ValidClickEvent_ReturnsAccepted() {
        ClickEvent event = new ClickEvent();
        event.setEventType("click");
        event.setEventId("evt-1");
        event.setUserId("user-1");
        event.setTimestamp(1000L);
        event.setAdId("ad-1");
        event.setCampaignId("camp-1");
        event.setSource("test");

        when(eventProducer.sendEvent(any())).thenReturn(Mono.empty());

        webTestClient.post()
                .uri("/api/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(event)
                .exchange()
                .expectStatus().isAccepted();
        verify(eventProducer).sendEvent(any(ClickEvent.class));
    }

    @Test
    void ingestEvent_InvalidEvent_ReturnsBadRequest() {
        ClickEvent event = new ClickEvent();
        // Missing required fields

        webTestClient.post()
                .uri("/api/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(event)
                .exchange()
                .expectStatus().isBadRequest();
    }
}
