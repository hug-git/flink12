package com.hug.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink10_Transform_RichFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> input = env.readTextFile("input");

        SingleOutputStreamOperator<String> filter = input.filter(new MyRichFilterFun());

        filter.print();

        env.execute();


    }

    public static class MyRichFilterFun extends RichFilterFunction<String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open()...");
        }

        @Override
        public boolean filter(String value) throws Exception {
            return value.startsWith("hello");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close()...");
        }
    }
}
