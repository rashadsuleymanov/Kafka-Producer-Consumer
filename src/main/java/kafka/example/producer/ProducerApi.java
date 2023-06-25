package kafka.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.*;

public class ProducerApi {

    public static void main(String[] args) {
        // Properties teyin etmek
        Properties properties = new Properties();
        //Manual config
//        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("key.serializer", IntegerSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
        // Real config
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer<K, V> obyektini yaratmaq
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
        // ProduceRecord obyektini yaratmaq
        for (int i = 1; i < 5; i++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<>("my-first-topic", i, "Msg from Java #" + i);
            producer.send(record);
        }
        // Mesaj gondermek
        // Flush yaxud close
        producer.close();

    }

}
