# Flink Processor Tests

This directory contains comprehensive unit tests for the Flink processor module.

## Test Structure

```
src/test/java/com/example/adaggregator/flink/
├── AttributionProcessFunctionTest.java          # Core attribution logic tests
├── model/
│   └── ModelTest.java                           # Data model tests
└── serialization/
    ├── EventDeserializationSchemaTest.java      # Kafka deserialization tests
    └── AttributedEventSerializationSchemaTest.java  # Kafka serialization tests
```

## Running Tests

### All Tests
```bash
./gradlew test
```

### Specific Test Class
```bash
./gradlew test --tests AttributionProcessFunctionTest
./gradlew test --tests EventDeserializationSchemaTest
./gradlew test --tests AttributedEventSerializationSchemaTest
./gradlew test --tests ModelTest
```

### With Verbose Output
```bash
./gradlew test --info
```

### View HTML Report
```bash
./gradlew test
open build/reports/tests/test/index.html
```

## Test Coverage

| Component | Tests | What's Tested |
|-----------|-------|---------------|
| **AttributionProcessFunction** | 8 | Attribution logic, state management, window boundaries |
| **EventDeserializationSchema** | 8 | JSON to Event deserialization, polymorphism, error handling |
| **AttributedEventSerializationSchema** | 6 | Event to JSON serialization, field mapping |
| **Model Classes** | 10 | Data models, JSON annotations, equality |
| **Total** | **32** | **All major components** |

## Key Test Scenarios

### Attribution Logic
- ✅ Click storage in state
- ✅ Successful attribution within 24-hour window
- ✅ Attribution window boundary validation
- ✅ Duplicate conversion handling
- ✅ Multi-campaign attribution
- ✅ Temporal ordering (conversion after click)

### Serialization/Deserialization
- ✅ Polymorphic event type resolution
- ✅ JSON field mapping (snake_case)
- ✅ Error handling for invalid JSON
- ✅ BigDecimal precision for monetary values

### Edge Cases
- ✅ Conversions outside attribution window
- ✅ Conversions before clicks
- ✅ Missing matching clicks
- ✅ Zero-value conversions
- ✅ Large monetary values

## Dependencies

Tests use:
- **JUnit Jupiter 5.10.0** - Test framework
- **AssertJ 3.24.2** - Fluent assertions
- **Flink Test Utils** - Flink testing harness

## Java 17+ Compatibility

The build configuration includes JVM arguments to handle Java module access restrictions:

```gradle
test {
    jvmArgs = [
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        // ... other module opens
    ]
}
```

This is required for Flink's serialization framework to work with Java 17+.

## Writing New Tests

### Testing Stateful Functions

Use `KeyedOneInputStreamOperatorTestHarness` for testing `KeyedProcessFunction`:

```java
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
void testExample() throws Exception {
    // Process events
    testHarness.processElement(event, timestamp);
    
    // Verify output
    List<AttributedEvent> output = testHarness.extractOutputValues();
    assertThat(output).hasSize(1);
}
```

### Testing Serialization

```java
@Test
void testSerialization() throws IOException {
    AttributedEvent event = AttributedEvent.builder()
        .conversionId("conv-1")
        // ... other fields
        .build();
    
    byte[] serialized = schema.serialize(event);
    JsonNode json = objectMapper.readTree(serialized);
    
    assertThat(json.get("conversion_id").asText()).isEqualTo("conv-1");
}
```

## Continuous Integration

These tests are designed to run in CI/CD pipelines:
- No external dependencies (embedded Flink test harness)
- Fast execution (< 10 seconds)
- Deterministic results
- Clear failure messages

## Test Reports

After running tests, reports are available at:
- **HTML Report**: `build/reports/tests/test/index.html`
- **XML Results**: `build/test-results/test/*.xml`
- **Problems Report**: `build/reports/problems/problems-report.html`
