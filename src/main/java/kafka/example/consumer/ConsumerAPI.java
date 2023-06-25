package kafka.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;

public class ConsumerAPI {

    public static void main(String[] args) {
        // Properties teyin etmek
        Properties properties = new Properties();
        //Manual config
//        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("key.serializer", IntegerSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
        // Real config
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "cons-group-3");

        // KafkaConsumer<K, V> obyektini yaratmaq
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(properties);
        // Topic-lerin siyahisina abune olmaq (subscribe)

        consumer.subscribe(Arrays.asList("my-first-topic"));

        try {

            while (true) {
                // Poll
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord record : records) {
                    String message = record.value().toString();
                    String topic = record.topic();
                    int partition = record.partition();
                    System.out.println("DATA: " + message + " TOPIC: " + topic + " PARTITION: " + partition);
                }
                consumer.commitAsync();
            }

            // Consumer-i close() etmek

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                consumer.commitSync();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            consumer.close();
        }
    }
}
