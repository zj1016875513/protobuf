package com.zj.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class KafkaProducerMypartition {


    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

//        properties.put("partitioner.class", "com.kafka.partitions.MyPartitioner");  // 配置 自定义分区类
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            // 分区策略第一种： 如果即没有指定分区号，也没有指定数据key，那么就会使用轮询的方式将数据均匀的发送到不同
            ProducerRecord producerRecord1 = new ProducerRecord<>("ttt","mymessage"+i);
            producer.send(producerRecord1);
            //  分区策略第二种 如果没有指定分区号， 指定了数据key 通过key,hashcode  %  numPartitions 来计算数据究竟会保存在哪个分区
            // 注意 如果数据key，没有变化 key.hashcode % numPartitions = 固定值  所有的数据都会写到某一个分区里面去
//            ProducerRecord producerRecord2 = new ProducerRecord<>("mypartition","mykey","mymessage"+i);
//            producer.send(producerRecord2);

            // 第三种 分区策略： 如果指定了分区号，那么就会将数据直接写入到对应的分区里面去
//            ProducerRecord producerRecord3 = new ProducerRecord<>("mypartition",2,"mykey","mymessage"+i);
//            producer.send(producerRecord3);


            // 第四中 分区策略 自定义分区策略
//            ProducerRecord producerRecord4 = new ProducerRecord<>("mypartition","mymessage"+i);
//            producer.send(producerRecord4);
        }
        producer.close();
    }

}

//需求
//        kafka集群环境搭建完成后，整合到项目开发中，我需要给某个topic发送消息，以及监听消费该topic中的消息，难道我需要事先用kafka命令先去服务器创建一个topic，然后再供项目使用吗？
//        有没有我在项目配置文件里指定kafka的topic=xxx，然后启动服务的时候，kafka就可以自动帮我创建好呢？
//
//        解决
//        其实，在配置文件里指定好kafka的topic之后，调用send方法会自动帮我们创建好topic，只是创建的topic默认是1个副本和1个分区的，这一般不能满足我们的要求，所以我们还需要在kafka的server.properties里增加或修改以下参数：
//
//        num.partitions=3
//        auto.create.topics.enable=true
//        default.replication.factor=3
//
//        之后，kafka自动帮我们创建的主题都会包含3个副本和3个分区。
//        另外，也可以通过一些api帮我们创建好主题，这个就需要自己手动去实现创建topic的方法。
