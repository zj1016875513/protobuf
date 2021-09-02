package com.zj.kafka;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zj.proto.VIP;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class kfconsumer {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("protoTest"));
        while (true){
            ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : poll) {
//                System.out.println(record.toString());
                byte[] value = record.value();
                VIP.User user = VIP.User.parseFrom(value);
                String name = user.getName();
                int age = user.getAge();
                String address = user.getAddress();
                Person person = new Person(name, age, address);
                String s = JSON.toJSONString(person);
                System.out.println(s);
//                System.out.println(user.getName());

            }
        }
    }
}
