package com.zj.kafka;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class kafkaUtil {
    public static FlinkKafkaConsumer<byte[]> getKafkaSource(String groupid, String topic, String kafkaservers){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",kafkaservers);
        props.setProperty("group.id",groupid);
        props.setProperty("key.deserializer",String.valueOf(ByteArrayDeserializer.class));
        props.setProperty("value.deserializer",String.valueOf(ByteArrayDeserializer.class));
//        props.setProperty("isolation.level","read_committed");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.offset.reset","earliest");
        props.setProperty("session.timeout.ms","240000");
        props.setProperty("fetch.max.wait.ms","240000");
        props.setProperty("request.timeout.ms","250000");

        return new FlinkKafkaConsumer<>(topic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] value) throws IOException {
                return value;
            }
        },props);
    }
}
