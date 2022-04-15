package com.hug.day10Table;

import com.hug.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_StreamToTable_Agg {
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
//        Table selectTable = sensorTable.
//                where($("vc").isGreaterOrEqual(20))
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("sum_vc"))
//                .select($("id"), $("sum_vc"));

        Table selectTable = sensorTable.where("vc>=20").groupBy("id").select("id,id.count");

        selectTable.printSchema();

        // 将selectTable转换为流进行输出
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(selectTable, Row.class);
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(selectTable, Row.class);
        tuple2DataStream.print();

        env.execute();

    }
}
