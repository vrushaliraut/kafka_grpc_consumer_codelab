package com.vrushali.marvel.repository;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaRepository {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String GROUP_ID = "group.id";
    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";
    private static final String VALUE = "value";
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;

    public KafkaRepository(String groupId, List<String> topics) {
        this.topics = topics;
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, "localhost:9092");
        props.put(GROUP_ID, groupId);
        props.put(KEY_DESERIALIZER, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    public String consumeMessage() {
        Map<String, Object> data = new HashMap<>();
        try {
            consumer.subscribe(topics);

            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                data.put(PARTITION, record.partition());
                data.put(OFFSET, record.offset());
                data.put(VALUE, record.value());
                System.out.println("consume message successfully" + record.value());
            }

        } finally {
            consumer.close();
        }
        return String.valueOf(data.get(VALUE));
    }
}
