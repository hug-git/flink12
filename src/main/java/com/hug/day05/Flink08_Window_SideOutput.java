package com.hug.day05;

import com.hug.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink08_Window_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 使用ProcessFunction将数据分流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.process(new SplitProcessFunc());

        DataStream<WaterSensor> sideOut = result.getSideOutput(new OutputTag<WaterSensor>("SideOut"));

        sideOut.print("Side");

        result.print();

        env.execute();
    }
    // 自定义周期性的Watermark生成器
    public static class SplitProcessFunc extends ProcessFunction<WaterSensor, WaterSensor> {

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            // 取出水位线
            Integer vc = value.getVc();

            // 根据水位线高低，分流
            if (vc >= 30) {
                // 将数据输出至主流
                out.collect(value);
            } else {
                // 将数据输出至侧输出流
                ctx.output(new OutputTag<Tuple2<String, Integer>>("SideOut") {},
                        new Tuple2<>(value.getId(), vc));
            }

            // 定时器
            long ts = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerEventTimeTimer(ts + 5000L);
        }

        // 定时器出发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
            System.out.println("Timer is triggered");
        }
    }
}
