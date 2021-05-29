package com.hug.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> input = env.readTextFile("input");

        SingleOutputStreamOperator<String> map = input.map(new MyRichMapFun());

        map.print();

        env.execute();
    }

    public static class MyRichMapFun extends RichMapFunction<String, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open方法被调用");
        }

        @Override
        public String map(String value) throws Exception {
            return value + "-myMap";
        }

        @Override
        public void close() throws Exception {
            System.out.println("close方法被调用");
        }
    }
}
