package org.rusty_confluent.test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumeAndPrint {

    public static void main(String[] args) {
        Properties props = new Properties();
        String bootstrapServers = "localhost:9092";
        String groupId = "rusty-confluent-java-test";
        String schemaRegistryUrl = "http://localhost:8081";
        String topic = "TEST1";

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);


        try(KafkaConsumer consumer = new KafkaConsumer(props)){
            consumer.subscribe(Collections.singletonList(topic));
            IntStream.range(1, 11).forEach(loop -> print(loop, consumer));
        }
    }

    private static void print(int loop, KafkaConsumer consumer){
        System.out.println("Doing a poll for the " + loop + " time");
        ConsumerRecords consumerRecords = consumer.poll(Duration.ofSeconds(1));
        System.out.println("Found " + consumerRecords.count() + " records");
        consumerRecords.iterator().forEachRemaining(
            System.out::println
        );
    }
}
