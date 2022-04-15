package com.hug.practice;

import com.hug.bean.UserBehavior;
import com.hug.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.HashSet;

public class Flink_UserVisitor_BloomFilter {

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

        // 使用布隆过滤器 自定义触发器: 来一条计算一条(访问Redis一次)
        SingleOutputStreamOperator<UserVisitorCount> result = windowedStream
                .trigger(new MyTrigger())
                .process(new UserVisitorWindowFunc());
        result.print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE; // 计算并输出结果
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UserVisitorWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {

        private Jedis jedis;

        private MyBloomFilter myBloomFilter;

        // 声明每个窗口总人数的Key
        private String hourUvCountKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
            myBloomFilter = new MyBloomFilter(1 << 30);
            hourUvCountKey = "HourUv";
        }

        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            // 取出数据
            UserBehavior userBehavior = elements.iterator().next();

            // 提取窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            // 定义当前窗口的BitMap Key
            String bitMapKey = "BitMap_" + windowEnd;

            long offset = myBloomFilter.getOffset(userBehavior.getUserId().toString());
            Boolean exist = jedis.getbit(bitMapKey, offset);

            if (!exist) {
                // 将对应offset位置改为1
                jedis.setbit(bitMapKey, offset, true);

                // 累加当前窗口的总和
                jedis.hincrBy(hourUvCountKey, windowEnd, 1L);

                // 输出数据
                out.collect(new UserVisitorCount("UV", windowEnd, Integer.parseInt(jedis.hget(hourUvCountKey, windowEnd))));
            }
        }
    }

    // 自定义布隆过滤器
    public static class MyBloomFilter {
        // 容量
        private long cap;

        public MyBloomFilter(long cap) {
            this.cap = cap;
        }

        // 传入一个字符串，获取在BitMap中的位置
        public long getOffset(String value) {
            long result = 0L;

            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }

            // 取模
            return result & (cap - 1);
        }

    }

}
