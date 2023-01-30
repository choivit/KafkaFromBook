package com.example.kafkaFromBook.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// 동기 수신
public class ConsumerSync {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","peter-kafka01.foo.bar:9092,peter-kafka02.foo.bar:9092,peter-kafka02.foo.bar:9092");
        props.put("group.id", "peter-consumer01");
        props.put("enable.auto.commit","false"); // offset 자동 commit을 하지않도록 설정.
        props.put("auto.offset.reset","latest");
        props.put("key.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("peter-basic01")); // Consumer가 구독할 Topic을 지정한다.

        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic : %s , Partition : %d , Offset : %d, Key : %s, Value :%s\n",
                            record.topic(),record.partition(),record.offset(),record.key(),record.value());
                }
                // 추가 polling 전에 offset을 commit 함. 비동기는 consumer.commitAsync();로 동기와 다름.
                consumer.commitSync();
            }

        } catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }
}
