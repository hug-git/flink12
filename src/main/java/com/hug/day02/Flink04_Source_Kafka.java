package com.hug.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink01");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        kafkaDS.print();

        env.execute();
    }
}
