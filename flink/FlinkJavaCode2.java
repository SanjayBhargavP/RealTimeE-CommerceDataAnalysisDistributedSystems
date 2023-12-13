import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class CouchbaseSink implements SinkFunction<ProductCount> {
    
    private transient Cluster cluster;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Initialize Couchbase connection
        cluster = CouchbaseCluster.create("couchbase_host"); // Replace with your Couchbase host
        cluster.authenticate("username", "password"); // Replace with your credentials
        cluster.openBucket("bucketName"); // Replace with your bucket name
    }

    @Override
    public void invoke(ProductCount count, Context context) throws Exception {
        Bucket bucket = cluster.bucket("bucketName");
        String documentId = "product::" + count.productId;

        // Try to retrieve the existing document
        JsonDocument existingDoc = bucket.get(documentId);
        if (existingDoc != null) {
            // Document exists, update counts
            JsonObject existingContent = existingDoc.content();
            long newViewCount = existingContent.getLong("viewCount") + count.viewCount;
            long newPurchaseCount = existingContent.getLong("purchaseCount") + count.purchaseCount;

            existingContent.put("viewCount", newViewCount)
                          .put("purchaseCount", newPurchaseCount);
            bucket.upsert(JsonDocument.create(documentId, existingContent));
        } else {
            // Document does not exist, create new
            JsonObject json = JsonObject.create()
                .put("productId", count.productId)
                .put("viewCount", count.viewCount)
                .put("purchaseCount", count.purchaseCount);

            JsonDocument doc = JsonDocument.create(documentId, json);
            bucket.upsert(doc);
        }
    }

    @Override
    public void close() throws Exception {
        if (cluster != null) {
            cluster.disconnect();
        }
        super.close();
    }
}
