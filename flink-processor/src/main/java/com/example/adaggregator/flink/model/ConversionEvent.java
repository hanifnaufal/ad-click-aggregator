package com.example.adaggregator.flink.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import java.math.BigDecimal;

@Data
@EqualsAndHashCode(callSuper = true)
public class ConversionEvent extends Event {
    private String type;
    private BigDecimal value;
    private String source;
}
