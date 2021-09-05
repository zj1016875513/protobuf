package com.zj.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo1", "hello"+ i);//topic，value
            producer.send(record);
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i))); //topic, key, value
        }

        producer.close();//不要忘记close
    }
}
