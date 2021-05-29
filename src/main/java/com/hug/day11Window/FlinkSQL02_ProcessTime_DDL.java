package com.hug.day11Window;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_ProcessTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册source_sensor
        tableEnv.executeSql("create table source_sensor (id string,ts bigint,vc int,pt as PROCTIME()) with (" +
                "'connector' = 'kafka'" +
                "'topic' = 'topic_source'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");

        Table table = tableEnv.from("source_sensor");

        // 打印元数据信息
        table.printSchema();

    }
}
