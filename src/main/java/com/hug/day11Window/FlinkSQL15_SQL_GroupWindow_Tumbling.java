package com.hug.day11Window;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL15_SQL_GroupWindow_Tumbling {
    public static void main(String[] args) {
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

        // SQL API 实现滚动时间窗口
        Table result = tableEnv.sqlQuery("select" +
                "id," +
                "count(id)," +
                "tumble_start(pt, INTERVAL '5' second) as windowStart from" +
                table +
                "group by id,tumble(pt, INTERVAL '5' second)");
        // SQL API 实现滑动时间窗口
        Table result1 = tableEnv.sqlQuery("select" +
                "id," +
                "count(id)," +
                "hop_start(pt, INTERVAL '2' second, INTERVAL '5' second) as windowStart from" +
                table +
                "group by id,hop(pt, INTERVAL '2' second, INTERVAL '5' second)");
        // SQL API 实现会话时间窗口
        Table result2 = tableEnv.sqlQuery("select" +
                "id," +
                "count(id)," +
                "session_start(pt, INTERVAL '5' second) as windowStart from" +
                table +
                "group by id,hop(pt, INTERVAL '5' second)");
        // SQL API 实现Over窗口
        Table result3 = tableEnv.sqlQuery("select" +
                "id," +
                "sum(vc) over w as sum_vc)," +
                "count(id) over w as ct " +
                "from " + table +
                " window w as (partition by id order by pt");
        // FlinkSQL中只能有一种Over窗口
        Table result4 = tableEnv.sqlQuery("select" +
                "id," +
                "sum(vc) over (order by pt) as sum_vc)," +
                "count(id) over (order by pt) as ct " +
                "from " + table);

        // 将结果表转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();
    }
}
