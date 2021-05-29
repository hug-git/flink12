package com.hug.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> map = socketTextStream.map(x -> x).setParallelism(2);

        // 使用不同的重分区策略
        socketTextStream.keyBy(data -> data).print("KeyBy");
        socketTextStream.shuffle().print("Shuffle");
        socketTextStream.rebalance().print("Rebalance"); // 每个分区针对下个所有分区轮循
        socketTextStream.rescale().print("Scale"); // 每个分区针对下个平均分配分区轮循
        socketTextStream.global().print("Global");
//        socketTextStream.forward().print("Forward"); // error
        socketTextStream.broadcast().print("Broadcast");

        env.execute();
    }
}
