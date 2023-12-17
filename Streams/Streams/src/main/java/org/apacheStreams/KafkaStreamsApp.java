package org.apacheStreams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "csv-app-aggonemssad-azpppaadzssdaaa");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092"); // Change to your Kafka server
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build the topology
        StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the Kafka topic
        KStream<String, String> stream = builder.stream("views"); // Change to your topic name

        // Process each record (just printing in this example)
//        stream.foreach((key, value) -> System.out.println("Key: " + key + ", Value: " + value));

        KStream<String, String> rekeyedStream = stream.mapValues(value -> {
            String[] parts = value.replace("\"", "").split(",");
            if (parts.length >= 9) {
                return parts[2]; // Extract productId
            } else {
                return null; // Or handle the error as appropriate
            }
        }).filter((key, value) -> value != null);



//        rekeyedStream.foreach((key, productId) -> System.out.println("Original Key: " + key + ", ProductId: " + productId));

        KStream<String, String> streamWithProductIdKey = rekeyedStream.selectKey((key, value) -> value);

        streamWithProductIdKey.foreach((key, productId) -> System.out.println("Original Key: " + key + ", ProductId: " + productId));

        KStream<String, Long> KS1=streamWithProductIdKey.map((key, inv) -> new KeyValue<>(
                key,
                1L
        ));
        KS1.foreach((key, count) -> System.out.println(" Key: " + key + ", Count: " + count));

        KGroupedStream<String, Long> KGS0 = KS1.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

// Count the occurrences for each key
//        KTable<String, Long> counts = KGS0.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")
//                .withKeySerde(Serdes.String())
//                .withValueSerde(Serdes.Long()));

// Optionally, print the counts
        //counts.toStream().foreach((key, value) -> System.out.println("Key: " + key + ", Count: " + value));



// Print the contents of KT0
//        KTable<String, Long> wordCounts = KS1
//                .groupByKey()
//                .count();
//        wordCounts.toStream().foreach((key, value) -> {
//            System.out.println("Product: " + key + ", Count: " + value);
//       });
//        KTable<String, Long> KT0 = KS1.reduce((aggValue, newValue) -> {
//            newValue.setTotalLoyaltyPoints(newValue.getEarnedLoyaltyPoints() + aggValue.getTotalLoyaltyPoints());
//            return newValue;
//        });

//        KGroupedStream<String, String> KGS0=streamWithProductIdKey.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

//        KTable<String, Long> productCounts = streamWithProductIdKey
//                .groupBy((key, value) -> key, Grouped.with(Serdes.String(), Serdes.String()))
//                .aggregate(
//                        () -> 0L, // Initializer
//                        (key, value, aggregate) -> aggregate + 1L, // Aggregator
//                        Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("product-counts-store")
//                                .withKeySerde(Serdes.String())
//                                .withValueSerde(Serdes.Long())
//                );
//        productCounts.toStream().foreach((key, value) -> {
//            System.out.println("Product: " + key + ", Count: " + value);
//        });





// Output the result to a new Kafka topic
//        productCounts.toStream().to("product-transaction-frequency", Produced.with(Serdes.String(), Serdes.Long()));


//        KGroupedStream<String, String> groupedStream = rekeyedStream.groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String()));
//        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Long()))
//                .toStream()
//                .foreach((productId, count) -> System.out.println("ProductId: " + productId + ", Count: " + count));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "csv-app-aggone");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CsvRecordSerde.class);
//
//        System.out.println("Starting Kafka Streams Application");
//
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> sourceStream = builder.stream("views", Consumed.with(Serdes.String(), Serdes.String()));
//        sourceStream.foreach((key, value) -> {
//            System.out.println("Key: " + key + ", Value: " + value);
//        });
//
//        System.out.println("Created source stream from topic 'views'");
//
//        KTable<String, Long> aggregatedStream = sourceStream
//                .groupBy((key, value) -> {
//                    String[] val = value.split(",");
//                    System.out.println("Grouping by Product ID: " + val[3]);
//                    return val[3];
//                }, Grouped.with(Serdes.String(), Serdes.String()))
//                .count();
//
//        System.out.println("Aggregated stream by Product ID");
//
//        aggregatedStream.toStream().foreach((key, value) -> {
//            System.out.println("Product ID: " + key + ", Count: " + value);
//        });
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), props);
//
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        // Attach shutdown handler to catch control-c
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
//                System.out.println("Shutting down stream");
//                streams.close();
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            System.out.println("Kafka Streams Application Started");
//            latch.await();  // Wait until the latch is decremented
//        } catch (Throwable e) {
//            System.exit(1);
//        }
//    }
//
//    public static class CsvRecordSerde implements Serde<CsvRecord> {
//        @Override
//        public Serializer<CsvRecord> serializer() {
//            return new CsvRecordSerializer();
//        }
//
//        @Override
//        public Deserializer<CsvRecord> deserializer() {
//            return new CsvRecordDeserializer();
//        }
//    }
}
