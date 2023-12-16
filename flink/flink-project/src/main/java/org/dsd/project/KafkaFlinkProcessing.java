package org.dsd.project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class KafkaFlinkProcessing {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set properties for the Kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-1:9092");
        properties.setProperty("group.id", "flink-consumer-group");

        // Define Kafka source for views
        FlinkKafkaConsumer<String> viewsConsumer = new FlinkKafkaConsumer<>(
                "views",
                new SimpleStringSchema(),
                properties);

        // Define Kafka source for purchases
        FlinkKafkaConsumer<String> purchasesConsumer = new FlinkKafkaConsumer<>(
                "purchases",
                new SimpleStringSchema(),
                properties);

        // Create data streams for views and purchases
        DataStream<String> viewsStream = env.addSource(viewsConsumer);
        DataStream<String> purchasesStream = env.addSource(purchasesConsumer);

        // Process the streams to transform the raw data into ProductInfo objects
        DataStream<ProductInfo> processedStream = viewsStream
                .union(purchasesStream)
                .map(new MapFunction<String, ProductInfo>() {
                    @Override
                    public ProductInfo map(String value) throws Exception {
                        String[] fields = value.split(",");
                        String eventType = fields[1].trim();
                        String productId = fields[2].trim();
                        String categoryCode = fields[4].trim().isEmpty() ? null : fields[4].trim();
                        String brand = fields[5].trim().isEmpty() ? null : fields[5].trim();

                        return new ProductInfo(productId, categoryCode, brand, eventType);
                    }
                });

        // Define the windowing criteria for aggregation
        DataStream<String> aggregatedStream = processedStream
                .keyBy(ProductInfo::getProductId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new TopProductsAggregateFunction());

        // Define Kafka producers for the two topics
        FlinkKafkaProducer<String> viewsProducer = new FlinkKafkaProducer<>(
                "top-views",
                new SimpleStringSchema(),
                properties);

        FlinkKafkaProducer<String> purchasesProducer = new FlinkKafkaProducer<>(
                "top-purchases",
                new SimpleStringSchema(),
                properties);

        // Split the stream based on the type and write to the respective topics
        aggregatedStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) {
                        // Split the aggregated data into views and purchases
                        String[] parts = value.split(" \\| ");
                        if (parts.length == 2) {
                            out.collect(new Tuple2<>("views", parts[0]));
                            out.collect(new Tuple2<>("purchases", parts[1]));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                        out.collect(value.f1);
                    }
                })
                .addSink(new FlinkKafkaProducer<>(
                        "top-views",         // The output topic for views
                        new SimpleStringSchema(), // Serializer for the string
                        properties
                ));

        aggregatedStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) {
                        // Split the aggregated data into views and purchases
                        String[] parts = value.split(" \\| ");
                        if (parts.length == 2) {
                            out.collect(new Tuple2<>("views", parts[0]));
                            out.collect(new Tuple2<>("purchases", parts[1]));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                        out.collect(value.f1);
                    }
                })
                .addSink(new FlinkKafkaProducer<>(
                        "top-purchases",      // The output topic for purchases
                        new SimpleStringSchema(), // Serializer for the string
                        properties
                ));

        // Execute the Flink job
        env.execute("Kafka Flink Streaming Processing");
    }
}
