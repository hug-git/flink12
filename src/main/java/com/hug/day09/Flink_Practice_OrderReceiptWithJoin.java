package com.hug.day09;

import com.hug.bean.OrderEvent;
import com.hug.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink_Practice_OrderReceiptWithJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> orderInput = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receiptInput = env.readTextFile("input/ReceiptLog.csv");

        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        WatermarkStrategy<TxEvent> txEventWatermarkStrategy = WatermarkStrategy.<TxEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
                    @Override
                    public long extractTimestamp(TxEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderEvent> orderDS = orderInput.map(OrderEvent::toBean)
                .filter((FilterFunction<OrderEvent>) value -> "pay".equals(value.getEventType()))
                .assignTimestampsAndWatermarks(orderEventWatermarkStrategy);
        SingleOutputStreamOperator<TxEvent> txDS = receiptInput.map(TxEvent::toBean)
                .assignTimestampsAndWatermarks(txEventWatermarkStrategy);


        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderDS.keyBy(OrderEvent::getTxId)
                .intervalJoin(txDS.keyBy(TxEvent::getTxId))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {
                    @Override
                    public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                        out.collect(Tuple2.of(left, right));
                    }
                });

        result.print();

        env.execute();
    }

}
