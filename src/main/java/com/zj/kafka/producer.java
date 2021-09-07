package com.zj.kafka;

import com.zj.testProto.VIP;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("ttt", "zijiang" + i));
//            kafkaProducer.send(new ProducerRecord<String, String>("first", "atguigu" + i)).get();
        }
        kafkaProducer.close();
    }

    @Test
    public void protobufProducer() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            VIP.User.Builder builder = VIP.User.newBuilder();
            builder.setId(i);
            builder.setName("zhangsan-"+i);
            builder.setAge(20+i);
            builder.setAddress("shenzhen-"+i);
            builder.setRegisterTime("2021-01-01");
            VIP.User user = builder.build();
            TimeUnit.MILLISECONDS.sleep(500);
//            kafkaProducer.send(new ProducerRecord<>("protoTest", user.getName().getBytes(StandardCharsets.UTF_8), user.toByteArray()));
//            kafkaProducer.send(new ProducerRecord<>("only1",null, user.toByteArray()));
            kafkaProducer.send(new ProducerRecord<>("protoTest", user.toByteArray()));
        }
        kafkaProducer.close();
    }
}
