package com.hug.practice;

import com.hug.bean.ItemCount;
import com.hug.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;

public class Flink_ItemCountTopN_Window {

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

        KeyedStream<Tuple2<Long, Integer>, Long> keyedStream = userBehaviorDS.map(data -> new Tuple2<Long, Integer>(data.getItemId(), 1))
                .returns(Types.TUPLE(Types.LONG, Types.INT))
                .keyBy(data -> data.f0);
        WindowedStream<Tuple2<Long, Integer>, Long, TimeWindow> windowedStream =
                keyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)));

        SingleOutputStreamOperator<ItemCount> aggregate =
                windowedStream.aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        SingleOutputStreamOperator<String> result = aggregate.keyBy(ItemCount::getTime)
                .process(new ItemCountProcessFunc(5));

        result.print();

        env.execute();
    }

    public static class ItemCountAggFunc implements AggregateFunction<Tuple2<Long, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<Long, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class ItemCountWindowFunc implements WindowFunction<Integer, ItemCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Integer> input, Collector<ItemCount> out) throws Exception {
            Integer next = input.iterator().next();
            String windowEnd = new Timestamp(window.getEnd()).toString();
            out.collect(new ItemCount(itemId, windowEnd, next));
        }
    }

    public static class ItemCountProcessFunc extends KeyedProcessFunction<String, ItemCount, String> {

        private ListState<ItemCount> listState;

        private Integer topN;

        public ItemCountProcessFunc(Integer topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemCount>("list-state", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);

            // 定义定时器
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ctx.timerService().registerEventTimeTimer(sdf.parse(value.getTime()).getTime() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemCount> iterator = listState.get().iterator();

            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);

            // 排序
            itemCounts.sort((i1, i2) -> i2.getCount() - i1.getCount());

            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间: ").append(new Timestamp(timestamp - 1000L)).append("\n");
            sb.append("---------------------------------\n");
            for (int i = 0; i < Math.min(topN, itemCounts.size()); i++) {
                sb.append(itemCounts.get(i)).append("\n");
            }
            sb.append("---------------------------------\n\n");
            out.collect(sb.toString());

            // 间隔时间打印
            Thread.sleep(1000);
        }
    }

}
