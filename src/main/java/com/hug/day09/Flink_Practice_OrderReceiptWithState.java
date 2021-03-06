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
            // ??????txEvent?????????
            TxEvent txEvent = txEventState.value();

            if (txEvent == null) { // txEvent??????
                // orderEvent???????????????
                orderEventState.update(value);

                // ??????????????????10s????????????
                long orderTime = (value.getEventTime() + 10) * 1000L;
                ctx.timerService().registerEventTimeTimer(orderTime);
                timerState.update(orderTime);

            } else { // txEvent????????????
                // ?????????
                out.collect(Tuple2.of(value, txEvent));

                //???????????????
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                // ????????????
                txEventState.clear();
                timerState.clear();
            }

        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // ??????order????????????
            OrderEvent orderEvent = orderEventState.value();

            if (orderEvent == null) {
                // ???txEvent???????????????
                txEventState.update(value);

                // ??????????????????5s????????????
                long txTime = (value.getEventTime() + 5) * 1000;
                ctx.timerService().registerEventTimeTimer(txTime);
                timerState.update(txTime);
            } else {
                out.collect(Tuple2.of(orderEvent, value));

                // ???????????????
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                // ????????????
                orderEventState.clear();
                timerState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // ??????order???tx?????????????????????????????????????????????
            OrderEvent orderEvent = orderEventState.value();
            TxEvent txEvent = txEventState.value();

            if (orderEvent != null) {
                ctx.output(new OutputTag<String>("Payed but not receipt"){}, orderEvent.getTxId() + "????????????????????????????????????" + timestamp);
            } else {
                ctx.output(new OutputTag<String>("Receipt but not Payed"){}, txEvent.getTxId() + "????????????????????????????????????" + timestamp);
            }
        }
    }
}
