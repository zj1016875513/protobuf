package com.zj.protobufTest;

import com.alibaba.fastjson.JSON;
import com.zj.kafka.Person;
import com.zj.kafka.kafkaUtil;
import com.zj.proto.VIP;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        String topic = "protoTest";
        String group = "protoTest";
        String kafkaservers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        env.addSource(kafkaUtil.getKafkaSource(group,topic,kafkaservers)).map(new MapFunction<byte[], String>() {
            @Override
            public String map(byte[] value) throws Exception {
                VIP.User user = VIP.User.parseFrom(value);
                String name = user.getName();
                int age = user.getAge();
                String address = user.getAddress();
                Person person = new Person(name, age, address);
                String s = JSON.toJSONString(person);
//                System.out.println(s);

                return s;
            }
        }).print();

        env.execute(FlinkKafkaTest.class.getSimpleName());
    }
}
