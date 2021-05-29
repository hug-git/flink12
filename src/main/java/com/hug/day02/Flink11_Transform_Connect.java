package com.hug.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink11_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDS = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> socketDS2 = env.socketTextStream("hadoop102", 9999);

        // 将socketTextStream2转换成Int类型
        SingleOutputStreamOperator<Integer> intDS = socketDS2.map(String::length);

        // 连接两个流
        ConnectedStreams<String, Integer> connectedStreams = stringDS.connect(intDS);

        // 处理连接之后的流
        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute();

    }
}
