package edu.rseymour.java.producer;

import edu.rseymour.java.model.PurchaseKey;
import edu.rseymour.java.partitioner.PurchaseKeyPartitioner;
import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");
        properties.put("partitioner.class",
                PurchaseKeyPartitioner.class.getName());

        PurchaseKey key = new PurchaseKey("12334568", new Date());

        try(Producer<PurchaseKey, String> producer =
                    new KafkaProducer<>(properties)) {
            ProducerRecord<PurchaseKey, String> record =
                    new ProducerRecord<>("transactions", key, "{\"item\":\"book\", \"price\":10.99}");

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Encountered exception " + exception);
                }
            };
            Future<RecordMetadata> sendFuture = producer.send(record, callback);
        }
    }

}
