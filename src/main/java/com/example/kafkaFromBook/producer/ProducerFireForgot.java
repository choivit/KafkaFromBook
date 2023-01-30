package com.example.kafkaFromBook.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

// 메시지를 보내고 확인하지 않기
public class ProducerFireForgot {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers","peter-kafka01.foo.bar:9092,peter-kafka02.foo.bar:9092,peter-kafka02.foo.bar:9092");
        props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            for(int i=0; i<3; i++){
                // Producer가 보낼 Record를 생성한다.
                ProducerRecord<String, String> record = new ProducerRecord<>("peter-basic01",
                        "Hello This is Kafka Record - " + i);
                producer.send(record);
            }
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }
}
