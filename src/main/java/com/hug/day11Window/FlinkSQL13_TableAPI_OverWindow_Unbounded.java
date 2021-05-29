package com.hug.day11Window;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkSQL13_TableAPI_OverWindow_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取文本数据创建流并转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                env.socketTextStream("hadoop102", 9999)
                        .map(WaterSensor::toBean);

        // 将流转换为表并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // 开启Over往前无界窗口
        Table result = table.window(Over.partitionBy($("id")).orderBy($("pt")).as("ow"))
                .select($("id"),
                        $("vc").sum().over("ow"),
                        $("id").count().over("ow"));
        // 开启Over往前两条数据窗口
        Table result1 = table.window(Over
                .partitionBy($("id"))
                .orderBy($("pt"))
                .preceding(rowInterval(2L))
                .as("ow"))
                .select($("id"),
                        $("vc").sum().over("ow"),
                        $("id").count().over("ow"));

        // 将结果表转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
