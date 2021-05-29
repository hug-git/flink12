package com.hug.practice;

import com.hug.bean.UserBehavior;
import com.hug.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class Flink_UserVisitor_Window {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.csv");

        // 转换为JavaBean, 并提取时间戳
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = readTextFile.map(UserBehavior::toBean)
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        // 按照行为分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDS.keyBy(UserBehavior::getBehavior);

        // 开窗
        WindowedStream<UserBehavior, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        // 使用HashSet方式
        SingleOutputStreamOperator<UserVisitorCount> result = windowedStream.process(new UserVisitorProcessWindowFunc());

        result.print();

        env.execute();
    }

    public static class UserVisitorProcessWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            HashSet<Long> uids = new HashSet<>();

            for (UserBehavior element : elements) {
                uids.add(element.getUserId());
            }

            // 输出数据
            out.collect(new UserVisitorCount("UV",
                    new Timestamp(context.window().getEnd()).toString(),
                    uids.size()));
        }
    }
}
