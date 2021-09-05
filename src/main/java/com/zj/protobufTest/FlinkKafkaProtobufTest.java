package com.zj.protobufTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.zj.kafka.Person;
import com.zj.kafka.kafkaUtil;
import com.zj.proto.VIP;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.util.Collector;

public class FlinkKafkaProtobufTest {
    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 20000);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "protoTest";
        String group = "protoTest";
        String kafkaservers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
//        env.addSource(kafkaUtil.getKafkaByteSource(group,topic,kafkaservers)).print(); //通过在集群上运行发现是可以消费数据输出内存地址的
/*        env.addSource(kafkaUtil.getKafkaByteSource(group,topic,kafkaservers))
                .map(new MapFunction<byte[], String>() {
            @Override
            public String map(byte[] value) throws Exception {
//                String s1 = new String(value);
//                System.out.println(s1);
                VIP.User user = VIP.User.parseFrom(value);
                String name = user.getName();
                int age = user.getAge();
                String address = user.getAddress();
//                Person person = new Person(name, age, address);
//                JSONObject result = new JSONObject();
//                result.put("name", name);
//                result.put("age", age);
//                result.put("address", address);
//                String s = result.toJSONString();


//                String s = JSON.toJSONString(person);
//                System.out.println(s);

//                String name1 = person.getName();
//                int age1 = person.getAge();
//                String address1 = person.getAddress();
//                String s = "name="+name1+";age="+age1+";address="+address1;

//                String s = "name="+name+";age="+age+";address="+address;
                String s = name+"\t"+age+"\t"+address;
                return s;
            }
        }).print();*/
        /*.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split("\t");
                String name = split[0];
                String age = split[1];
                String address = split[2];
                JSONObject result = new JSONObject();
                result.put("name", name);
                result.put("age", age);
                result.put("address", address);
                String outStr = result.toJSONString();

                return outStr;
            }
        }).print();*/

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        env.setStateBackend(new FsStateBackend("file:///D:/IDEAworkspace/flink_mytest/output/checkpoint"));
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/checkpoint"));
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig()
           .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.addSource(kafkaUtil.getKafkaByteSource(group,topic,kafkaservers))
                .process(new ProcessFunction<byte[],String>() {
                    @Override
                    public void processElement(byte[] value, Context ctx, Collector<String> out) throws Exception {
                        VIP.User user = VIP.User.parseFrom(value);
                        String name = user.getName();
                        int age = user.getAge();
                        String address = user.getAddress();
                        Person person = new Person(name, age, address);
                        Gson gson = new Gson();
                        String s1 = gson.toJson(person);
//                        String s = JSON.toJSONString(person);
                        out.collect(s1);
                    }
                }).print();

        env.execute(FlinkKafkaProtobufTest.class.getSimpleName());
    }
}
