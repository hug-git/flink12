package com.hug.day10Table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL12_MySQL {
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
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv'" +
                ")");

        // 注册since_sensor:MySQL
        tableEnv.executeSql("create table sink_sensor (id string,vc int, primary key(id) NOT ENFORCED) with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://hadoop102:3306/flink'," +
                "'table-name' = 'sensor_table'," +// 先在mysql中创建好users表
                "'username' = 'root'," +
                "'password' = '123456'" +
                ")");

        // 执行查询插入数据
        tableEnv.executeSql("insert into sink_sensor(id, vc) select id,CAST(count(*) as INT) as vc from source_sensor group by id");

        // SQL不需要执行该程序
//        env.execute();
    }
}
