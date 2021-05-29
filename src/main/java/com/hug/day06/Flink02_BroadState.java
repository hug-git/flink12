package com.hug.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_BroadState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> propertiesStream = env.socketTextStream("hadoop102", 8888);

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        BroadcastStream<String> broadcastStream = propertiesStream.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<String, String> connectedStream = socketTextStream.connect(broadcastStream);

        SingleOutputStreamOperator<String> result = connectedStream.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String aSwitch = broadcastState.get("Switch");

                if ("1".equals(aSwitch)) {
                    out.collect(value + "读取了广播状态，切换1");
                } else if ("2".equals(aSwitch)) {
                    out.collect(value + "读取了广播状态，切换2");
                } else {
                    out.collect(value + "读取了广播状态，切换其他");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put("Switch", value);
            }
        });

        result.print();

        env.execute();
    }
}
