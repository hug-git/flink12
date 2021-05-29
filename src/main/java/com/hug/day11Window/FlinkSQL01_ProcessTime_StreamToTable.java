package com.hug.day11Window;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_ProcessTime_StreamToTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取文本数据创建流并转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                env.readTextFile("input/sensor.txt")
                        .map(WaterSensor::toBean);

        // 将流转换为表，并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // 打印元数据信息
        table.printSchema();

    }
}
