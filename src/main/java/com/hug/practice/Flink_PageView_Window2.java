package com.hug.practice;

import com.hug.bean.PageViewCount;
import com.hug.bean.UserBehavior;
import java.sql.Timestamp;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

public class Flink_PageView_Window2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.csv");

        DataStreamSource<String> readTextFile = env.socketTextStream("hadoop102", 9999);
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = readTextFile.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(Long.valueOf(split[0]),
                    Long.valueOf(split[1]),
                    Integer.valueOf(split[2]),
                    split[3],
                    Long.valueOf(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        // 将数据转换为元祖
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvDS =
                userBehaviorDS.map(value -> new Tuple2<>("PV" + new Random().nextInt(8), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<PageViewCount> aggResult = pvDS.keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new PageViewAggFunc(), new PageViewWindowFunc());

        // 按照窗口信息重新分组，做第二次聚合
        SingleOutputStreamOperator<PageViewCount> result = aggResult.keyBy(PageViewCount::getTime)
                .sum("count");

        // 每个窗口只打印一次
//        SingleOutputStreamOperator<PageViewCount> result = aggResult.keyBy(PageViewCount::getTime)
//                .process(new KeyedProcessFunction<String, PageViewCount, PageViewCount>() {
//
//                    private ListState<PageViewCount> listState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        listState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("list-state", PageViewCount.class));
//                    }
//
//                    @Override
//                    public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
//                        listState.add(value);
//
//                        // 注册定时器
//                        String time = value.getTime();
//                        long ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
//                        ctx.timerService().registerEventTimeTimer(ts + 1);
//                    }
//
//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
//                        Integer count = 0;
//                        for (PageViewCount next : listState.get()) {
//                            count += next.getCount();
//                        }
//                        out.collect(new PageViewCount("PV", new Timestamp(timestamp - 1 ).toString(),count));
//                    }
//                });

        result.print();

        env.execute();
    }

    public static class PageViewAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
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

    public static class PageViewWindowFunc implements WindowFunction<Integer, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {
            // 提取窗口时间
            String ts = new Timestamp(window.getEnd()).toString();
            out.collect(new PageViewCount("PV", ts, input.iterator().next()));
        }
    }
}
