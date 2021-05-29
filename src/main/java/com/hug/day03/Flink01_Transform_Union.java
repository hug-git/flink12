package com.hug.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketDS1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> socketDS2 = env.socketTextStream("hadoop102", 9999);

        // 连接两条流
        DataStream<String> unionDS = socketDS1.union(socketDS2);

        unionDS.print();

        env.execute();
    }
}
