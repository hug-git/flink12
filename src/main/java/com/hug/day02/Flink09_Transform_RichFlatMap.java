package com.hug.day02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink09_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> input = env.readTextFile("input");

        SingleOutputStreamOperator<String> result = input.flatMap(new MyRichFlatMapFun());

        result.print();

        env.execute();
    }

    public static class MyRichFlatMapFun extends RichFlatMapFunction<String, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open()...");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] split = value.split(" ");
            for (String s : split) {
                out.collect(s);
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("close()...");
        }
    }
}
