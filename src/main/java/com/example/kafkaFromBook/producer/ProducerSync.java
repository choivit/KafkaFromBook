package com.example.kafkaFromBook.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

// 동기 전송
public class ProducerSync {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers","peter-kafka01.foo.bar:9092,peter-kafka02.foo.bar:9092,peter-kafka02.foo.bar:9092");
        props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            for(int i=0; i<3; i++){
                ProducerRecord<String, String> record = new ProducerRecord<>("peter-basic01",
                        "Hello This is Kafka Sync Record - " + i);
                // Producer는 Record를 전송하고 Broker로부터 응답을 기다림. Broker에서 에러가 발생하지 않으면 metadata를 얻음.
                RecordMetadata metadata = producer.send(record).get();
                // println이 아닌 printf임.
                System.out.printf("Topic : %s , Partition : %d , Offset : %d, Key : %s, Received Message :%s\n",
                        metadata.topic(),metadata.partition(),metadata.offset(),record.key(),record.value());
            }
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }
}
