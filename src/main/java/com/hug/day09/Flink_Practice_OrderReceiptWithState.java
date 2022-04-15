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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink_Practice_OrderReceiptWithState {
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


        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderDS.connect(txDS)
                .keyBy("txId", "txId")
                .process(new OrderTxKeyedProcessFunc());

        result.print();
        result.getSideOutput(new OutputTag<String>("Payed but not receipt") {}).print("Not Receipt");
        result.getSideOutput(new OutputTag<String>("Receipt but not Payed"){}).print("Not Payed");

        env.execute();
    }

    public static class OrderTxKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        private ValueState<OrderEvent> orderEventState;
        private ValueState<TxEvent> txEventState;
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class));
            txEventState = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent>("tx-state", TxEvent.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 判断txEvent是否到
            TxEvent txEvent = txEventState.value();

            if (txEvent == null) { // txEvent没到
                // orderEvent保存到状态
                orderEventState.update(value);

                // 创建定时器（10s后触发）
                long orderTime = (value.getEventTime() + 10) * 1000L;
                ctx.timerService().registerEventTimeTimer(orderTime);
                timerState.update(orderTime);

            } else { // txEvent已经到达
                // 写入流
                out.collect(Tuple2.of(value, txEvent));

                //删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                // 清空状态
                txEventState.clear();
                timerState.clear();
            }

        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 判断order是否到达
            OrderEvent orderEvent = orderEventState.value();

            if (orderEvent == null) {
                // 将txEvent更新到状态
                txEventState.update(value);

                // 注册定时器（5s后触发）
                long txTime = (value.getEventTime() + 5) * 1000;
                ctx.timerService().registerEventTimeTimer(txTime);
                timerState.update(txTime);
            } else {
                out.collect(Tuple2.of(orderEvent, value));

                // 删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                // 清空状态
                orderEventState.clear();
                timerState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 判断order和tx哪个状态不为空则输出到侧输出流
            OrderEvent orderEvent = orderEventState.value();
            TxEvent txEvent = txEventState.value();

            if (orderEvent != null) {
                ctx.output(new OutputTag<String>("Payed but not receipt"){}, orderEvent.getTxId() + "只有支付数据没有到账数据" + timestamp);
            } else {
                ctx.output(new OutputTag<String>("Receipt but not Payed"){}, txEvent.getTxId() + "只有到账数据没有支付数据" + timestamp);
            }
        }
    }
}
