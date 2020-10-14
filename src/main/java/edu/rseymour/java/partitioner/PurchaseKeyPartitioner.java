package edu.rseymour.java.partitioner;

import edu.rseymour.java.model.PurchaseKey;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

public class PurchaseKeyPartitioner extends DefaultPartitioner {

    @Override
    public int partition(String topic, Object key,
                         byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        Object newKey = null;

        // if the key is not null, extract the customer ID
        if (key != null) {
            PurchaseKey purchaseKey = (PurchaseKey) key;
            newKey = purchaseKey.getCustomerId();
            // sets the key bytes to the new value
            keyBytes = ((String) newKey).getBytes();
        }
        // returns the partition with the updated key, delegating to the superclass
        return super.partition(topic, newKey, keyBytes, value, valueBytes, cluster);
    }
}
