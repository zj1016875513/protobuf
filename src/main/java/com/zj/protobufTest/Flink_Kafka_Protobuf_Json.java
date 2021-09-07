package com.zj.protobufTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zj.kafka.Person;
import com.zj.kafka.kafkaUtil;
import com.zj.testProto.SmallStudent;
import com.zj.testProto.VIP;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// /opt/module/flink-yarn/bin/flink run -d -c com.zj.protobufTest.Flink_Kafka_Protobuf_Json /opt/module/jars/protobuf-1.0-SNAPSHOT.jar
// flink run -m yarn-cluster -ynm protobufprint /opt/module/jars/protobuf.jar


public class Flink_Kafka_Protobuf_Json {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "protoTest";
        String group = "protoTest";
        String kafkaservers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        env.addSource(kafkaUtil.getKafkaByteSource(group,topic,kafkaservers))
                .map(new MapFunction<byte[], String>() {
                    @Override
                    public String map(byte[] bytes) throws Exception {
                        SmallStudent smallStudent = SmallStudent.parseFrom(bytes);
                        String name = smallStudent.getName();
                        int age = smallStudent.getAge();
                        String address = smallStudent.getAddress();
                        Person person = new Person(name, age, address);
                        String result = JSON.toJSONString(person);
//                        String result = name+";"+age+";"+address;
//                        System.out.println(result);
                        return result;
                    }
                }).print();
        env.execute(Flink_Kafka_Protobuf_Json.class.getSimpleName());
    }
}
