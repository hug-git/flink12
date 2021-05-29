package com.hug.day09;

import com.hug.bean.LoginEvent;
import com.hug.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class Flink01_Practice_LoginFailWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> readTextFile = env.readTextFile("input/LoginLog.csv");

        // 转换为JavaBean, 并提取时间戳
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginEvent> loginEventDS = readTextFile.map(LoginEvent::toBean)
                .assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        // 定义模式
//        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).next("next").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).within(Time.seconds(2));

        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).times(2)
                .consecutive() // 严格近邻
                .within(Time.seconds(5));

        // 将模式序列作用于流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, loginEventPattern);

        // 提取匹配上的事件
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {

            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                // 取出数据
                LoginEvent start = pattern.get("start").get(0);
//                LoginEvent next = pattern.get("next").get(0);
                LoginEvent next = pattern.get("start").get(1);

                // 输出结果
                return start.getUserId() + "在 " + start.getEventTime()
                        + " 到 " + next.getEventTime() + " 之间连续登录失败两次。";
            }
        });

        result.print();

        env.execute();
    }
}
