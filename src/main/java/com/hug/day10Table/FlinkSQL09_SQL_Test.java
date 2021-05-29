package com.hug.day10Table;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL09_SQL_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(WaterSensor::toBean);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.使用未注册表
        // 将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 使用SQL查询未注册的表
        Table result = tableEnv.sqlQuery("select id,ts,vc from " + sensorTable + " where id = 'ws_001'");

        // 2.使用注册表
        tableEnv.createTemporaryView("sensor", waterSensorDS);

        Table result1 = tableEnv.sqlQuery("select id,ts,vc from sensor where id = 'ws_001'");

        // 将表对象转换为流进行打印输出
        tableEnv.toAppendStream(result, Row.class).print();


    }
}
