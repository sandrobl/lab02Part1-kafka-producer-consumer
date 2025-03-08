package com.example;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList("user-events", "global-events"));

        long overallDuration = 0;

        while (true) {
            long startTime = System.currentTimeMillis();

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    switch (record.topic()) {
                        case "user-events":
                            System.out.println("Received user-events message - key: " + record.key() + " value: " + record.value());
                            break;
                        case "global-events":
                            System.out.println("Received global-events message - value: " + record.value());
                            break;
                        default:
                            throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                    }
                }
                long duration = System.currentTimeMillis() - startTime;
                overallDuration += duration;
                if(overallDuration != 0){
                    System.out.println("duration: " + overallDuration + " ms");
                }
            }
        }
    }
}
