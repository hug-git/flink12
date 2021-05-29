package com.hug.day10Table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FinkSQL12_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册source_sensor
        tableEnv.executeSql("create table source_sensor (id string,vc int) with (" + "'connector' = 'kafka'" +
                "'topic' = 'topic_source'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");

        // 注册since_sensor:MySQL
        tableEnv.executeSql("create table sink_sensor (id string,vc int) with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://hadoop102:3306/test'," +
                "'table-name' = 'users'" + // 先在mysql中创建好users表
                ")");

        // 执行查询插入数据
        tableEnv.executeSql("insert into sink_sensor select * fromm source_sensor where id = 'ws_001'");

        env.execute();
    }
}
