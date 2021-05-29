package com.hug.day03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Transform_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // 使用Process实现压平功能
        SingleOutputStreamOperator<String> wordDS = socketTextStream.process(new ProcessFlatMapFunc());

        // 使用Process实现Map功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.process(new ProcessMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        keyedStream.print();

        env.execute();
    }

    public static class ProcessFlatMapFunc extends ProcessFunction<String, String>{

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }
    }
    public static class ProcessMapFunc extends ProcessFunction<String, Tuple2<String, Integer>>{

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<String, Integer>(value, 1));
        }
    }
}
