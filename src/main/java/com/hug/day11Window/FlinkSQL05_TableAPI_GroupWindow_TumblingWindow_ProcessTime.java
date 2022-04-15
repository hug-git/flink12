package com.hug.day11Window;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkSQL05_TableAPI_GroupWindow_TumblingWindow_ProcessTime {
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


        // 开滚动窗口计算WordCount
        Table result = table.window(Tumble.over(lit(5).seconds()).on($("pt")).as($("tw")))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count());
        // 开滑动窗口计算WordCount
        Table result1 = table.window(Slide.over(lit(5).seconds()).every((lit(2).seconds())).on($("pt")).as($("sw")))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());
        // 开会话窗口计算WordCount
        Table result2 = table.window(Session.withGap(lit(5).seconds()).on($("pt")).as($("sw")))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());
        // 开计数滚动窗口计算WordCount
        Table result3 = table.window(Tumble.over(rowInterval(5L)).on($("pt")).as($("cw")))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());
        // 开计数滑动窗口计算WordCount 注: 12版本sql初始5条才输出
        Table result4 = table.window(Slide.over(rowInterval(5L)).every((rowInterval(2L))).on($("pt")).as($("cw")))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());

        // 将结果表转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
