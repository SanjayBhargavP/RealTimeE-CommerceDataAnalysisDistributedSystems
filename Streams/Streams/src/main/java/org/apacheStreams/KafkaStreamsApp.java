package org.apacheStreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;
import java.util.Map;

// POJO for representing the key-value pair as JSON
class ProductCount {
    public String productId;
    public Long count;

    public ProductCount(String productId, Long count) {
        this.productId = productId;
        this.count = count;
    }
    // Getters and setters
}

// Custom Serde for the ProductCount class
class ProductCountSerde implements Serde<ProductCount> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<ProductCount> serializer() {
        return (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<ProductCount> deserializer() {
        return (topic, data) -> {
            try {
                return objectMapper.readValue(data, ProductCount.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Not needed for this example
    }

    @Override
    public void close() {
        // Not needed for this example
    }
}

public class KafkaStreamsApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "csv-app-oneeee");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> purchasesstream = builder.stream("purchases");
        KStream<String, String> viewStream = builder.stream("views");

        KTable<String, Long> productPurchaseCounts = purchasesstream
                .flatMapValues(value -> {
                    String[] parts = value.replace("\"", "").split(",");
                    if (parts.length >= 9) {
                        return java.util.Collections.singletonList(parts[2]);
                    } else {
                        return java.util.Collections.emptyList();
                    }
                })
                .groupBy((key, productId) -> productId, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("product-purchase-counts"));
        KTable<String, Long> productViewCounts = viewStream
                .flatMapValues(value -> {
                    String[] parts = value.replace("\"", "").split(",");
                    if (parts.length >= 9) {
                        return java.util.Collections.singletonList(parts[2]);
                    } else {
                        return java.util.Collections.emptyList();
                    }
                })
                .groupBy((key, productId) -> productId, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("product-view-counts"));

        // Convert the aggregated counts into JSON
        KStream<String, ProductCount> productPurchaseCountsJson = productPurchaseCounts.toStream()
                .map((key, value) -> KeyValue.pair(key, new ProductCount(key, value)));
        KStream<String, ProductCount> productViewCountsJson = productViewCounts.toStream()
                .map((key, value) -> KeyValue.pair(key, new ProductCount(key, value)));

        // Send the JSON data to the 'top-views' topic
        productPurchaseCountsJson.to("top-purchases", Produced.with(Serdes.String(), new ProductCountSerde()));
        productViewCountsJson.to("top-views", Produced.with(Serdes.String(), new ProductCountSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}