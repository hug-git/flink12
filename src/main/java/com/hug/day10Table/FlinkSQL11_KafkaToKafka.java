package com.hug.day10Table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL11_KafkaToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册source_sensor
        tableEnv.executeSql("create table source_sensor (id string,vc int) with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_source'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");


        // 注册sink_sensor
        tableEnv.executeSql("create table sink_sensor (id string,vc int) with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_sink'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");

        // 执行查询插入数据
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 'ws_001'");

    }
}
