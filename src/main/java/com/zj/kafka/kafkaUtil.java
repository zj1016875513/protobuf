package com.zj.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class kafkaUtil {
    public static FlinkKafkaConsumer010<String> getKafkaStringSource(String groupid, String topic, String kafkaservers){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",kafkaservers);
        props.setProperty("group.id",groupid);
//        props.setProperty("key.deserializer",String.valueOf(ByteArrayDeserializer.class));
//        props.setProperty("value.deserializer",String.valueOf(ByteArrayDeserializer.class));
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        props.setProperty("isolation.level","read_committed");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.offset.reset","earliest");
//        props.setProperty("auto.offset.reset","latest");
        props.setProperty("session.timeout.ms","240000");
        props.setProperty("fetch.max.wait.ms","240000");
        props.setProperty("request.timeout.ms","250000");

        FlinkKafkaConsumer010<String> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), props);

        return flinkKafkaConsumer;
    }

    public static FlinkKafkaConsumer010<byte[]> getKafkaByteSource(String groupid, String topic, String kafkaservers){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",kafkaservers);
        props.setProperty("group.id",groupid);
//        props.setProperty("key.deserializer",String.valueOf(ByteArrayDeserializer.class));
//        props.setProperty("value.deserializer",String.valueOf(ByteArrayDeserializer.class));
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        props.setProperty("isolation.level","read_committed");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.offset.reset","earliest");
//        props.setProperty("auto.offset.reset","latest");
        props.setProperty("session.timeout.ms","240000");
        props.setProperty("fetch.max.wait.ms","240000");
        props.setProperty("request.timeout.ms","250000");

        FlinkKafkaConsumer010<byte[]> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(topic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] value) throws IOException {
                return value;
            }
        }, props);

        return flinkKafkaConsumer;
    }

    public static <T> SinkFunction<T> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("transaction.timeout.ms", 14 * 60 * 1000 + "");
        return new FlinkKafkaProducer010<T>(
                topic,
                new SerializationSchema<T>() {
                    @Override
                    public byte[] serialize(T t) {
                        return t.toString().getBytes(StandardCharsets.UTF_8);
                    }
                },
                props
        );

    }
}
