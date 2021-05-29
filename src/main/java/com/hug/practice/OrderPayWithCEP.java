package com.hug.practice;

import com.hug.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderPayWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> readTextFile = env.readTextFile("input/OrderLog.csv");

        // 转换为JavaBean, 并提取时间戳
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderEvent> OrderEventDS = readTextFile.map(OrderEvent::toBean)
                .assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        KeyedStream<OrderEvent, Long> keyedStream = OrderEventDS.keyBy(OrderEvent::getOrderId);

        // 定义模式序列
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        // 将模式作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, orderEventPattern);

        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("No Pay") {},
                new OrderPayTimeOutFunc(),
                new OrderPaySelectFunc());

        result.print();
        result.getSideOutput(new OutputTag<String>("No Pay"){}).print("Time Out");

        env.execute();
    }

    public static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent, String> {

        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            OrderEvent create = pattern.get("start").get(0);

            return pattern.toString();
        }
    }

    public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent, String> {

        @Override
        public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
            OrderEvent create = pattern.get("start").get(0);
            OrderEvent follow = pattern.get("follow").get(0);

            return pattern.toString();
        }
    }
}
