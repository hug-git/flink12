package com.hug.day10Table;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class FlinkSQL07_Sink_ES {
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
        Table selectTable = sensorTable
                .where($("id").isEqual("ws_001")) // 用.groupBy($("id")) .count等聚合, 后面则用.inUpsertMode()
                .select($("id"), $("ts"), $("vc"));

        // 将selectTable写入ES
        tableEnv.connect(new Elasticsearch()
                .index("sensor_sql")
                .documentType("_doc")
                .version("6")
                .host("hadoop102", 9200, "http")
                .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Json())
                .inAppendMode()
                .createTemporaryTable("sensor");

        selectTable.executeInsert("sensor");


    }
}
