package com.zj.protobufTest;

import com.zj.kafka.kafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaStringTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "ttt";
        String group = "ttt";
        String kafkaservers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        env.addSource(kafkaUtil.getKafkaStringSource(group,topic,kafkaservers)).print();

        env.execute(FlinkKafkaProtobufTest.class.getSimpleName());
    }
}
