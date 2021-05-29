package com.hug.day10Table;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class FlinkSQL02_StreamToTable_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(WaterSensor::toBean);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 使用TableAPI过滤出"ws_001"的数据
        Table selectTable = sensorTable.where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"), $("vc"));

        sensorTable.where("id = 'ws_001'").select("id,ts,vc");

        selectTable.printSchema();

        // 将selectTable转换为流进行输出
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(selectTable, Row.class);
        rowDataStream.print();

        env.execute();

    }
}
