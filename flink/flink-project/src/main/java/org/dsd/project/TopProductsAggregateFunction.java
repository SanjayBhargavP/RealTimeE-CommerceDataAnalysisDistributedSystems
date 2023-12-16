package org.dsd.project;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class TopProductsAggregateFunction implements AggregateFunction<ProductInfo, Map<String, Tuple3<Integer, Integer, String>>, String> {

    @Override
    public Map<String, Tuple3<Integer, Integer, String>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Tuple3<Integer, Integer, String>> add(ProductInfo value, Map<String, Tuple3<Integer, Integer, String>> accumulator) {
        String productId = value.getProductId();
        Tuple3<Integer, Integer, String> counts = accumulator.getOrDefault(productId, new Tuple3<>(0, 0, value.getCategoryCode()));

        if ("view".equals(value.getEventType())) {
            counts.f0 += 1;  // Increment view count
        } else if ("purchase".equals(value.getEventType())) {
            counts.f1 += 1;  // Increment purchase count
        }

        accumulator.put(productId, counts);
        return accumulator;
    }

    @Override
    public String getResult(Map<String, Tuple3<Integer, Integer, String>> accumulator) {
        PriorityQueue<Tuple3<String, Integer, String>> viewQueue = new PriorityQueue<>((a, b) -> b.f1 - a.f1);
        PriorityQueue<Tuple3<String, Integer, String>> purchaseQueue = new PriorityQueue<>((a, b) -> b.f1 - a.f1);

        accumulator.forEach((id, counts) -> {
            viewQueue.add(new Tuple3<>(id, counts.f0, counts.f2));
            purchaseQueue.add(new Tuple3<>(id, counts.f1, counts.f2));
        });

        StringBuilder topViews = new StringBuilder("views:");
        StringBuilder topPurchases = new StringBuilder("purchases:");

        int count = 0;
        while (!viewQueue.isEmpty() && count < 5) {
            Tuple3<String, Integer, String> product = viewQueue.poll();
            topViews.append(product.f0).append(" (").append(product.f2).append("), ");
            count++;
        }

        count = 0;
        while (!purchaseQueue.isEmpty() && count < 5) {
            Tuple3<String, Integer, String> product = purchaseQueue.poll();
            topPurchases.append(product.f0).append(" (").append(product.f2).append("), ");
            count++;
        }

        return topViews.toString() + " | " + topPurchases.toString();
    }

    @Override
    public Map<String, Tuple3<Integer, Integer, String>> merge(Map<String, Tuple3<Integer, Integer, String>> a, Map<String, Tuple3<Integer, Integer, String>> b) {
        b.forEach((id, counts) -> a.merge(id, counts, (c1, c2) -> new Tuple3<>(c1.f0 + c2.f0, c1.f1 + c2.f1, c1.f2)));
        return a;
    }
}
