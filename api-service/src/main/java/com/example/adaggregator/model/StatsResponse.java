package com.example.adaggregator.model;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

@Data
@Builder
public class StatsResponse {
    private String interval;
    private List<StatsEntry> data;

    @Data
    @Builder
    public static class StatsEntry {
        private LocalDate date;
        private String campaignId;
        private String source;
        private long clicks;
        private long conversions;
        private BigDecimal revenue;
        private double cvr;
    }
}
