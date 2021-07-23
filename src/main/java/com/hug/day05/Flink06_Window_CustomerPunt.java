package com.hug.day05;

import com.hug.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink06_Window_CustomerPunt {
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

        // 提取数据中的时间戳字段
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod(2000L);
            }
        };

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator
                = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> keyedStream
                = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        // 开窗，时间间隔：指的是WaterMark跟(前一条)数据本身的时间差值，包含间隔时间
        WindowedStream<WaterSensor, String, TimeWindow> window
                = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");

        result.print();

        env.execute();
    }
    // 自定义间歇性的Watermark生成器
    public static class MyPeriod implements WatermarkGenerator<WaterSensor> {

        private Long maxTs;
        private Long maxDelay;

        public MyPeriod(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }

        // 数据到来时调用
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(eventTimestamp, maxTs);
            output.emitWatermark(new Watermark(maxTs - maxDelay));
        }

        // 周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }
}
