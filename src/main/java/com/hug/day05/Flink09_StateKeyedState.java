package com.hug.day05;

import com.hug.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class Flink09_StateKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new MyStateProcessFunc());

        result.print();

        env.execute();

    }

    public static class MyStateProcessFunc extends KeyedProcessFunction<String, WaterSensor, WaterSensor> {

        // 定义状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String, Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor, WaterSensor> aggregatingState;

        // 初始化


        @Override
        public void open(Configuration parameters) throws Exception {
            // 设置状态过期时间
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("value-state", Long.class);
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            stateDescriptor.enableTimeToLive(ttl);
            // 状态
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state", Long.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reducing-state", new ReduceFunction<WaterSensor>() {
                @Override
                public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                    return new WaterSensor(value1.getId(),value2.getTs(),value1.getVc());
                }
            }, WaterSensor.class));

            aggregatingState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<WaterSensor, Integer, WaterSensor>("aggregating-state", new AggregateFunction<WaterSensor, Integer, WaterSensor>() {
                @Override
                public Integer createAccumulator() {
                    return null;
                }

                @Override
                public Integer add(WaterSensor value, Integer accumulator) {
                    return null;
                }

                @Override
                public WaterSensor getResult(Integer accumulator) {
                    return null;
                }

                @Override
                public Integer merge(Integer a, Integer b) {
                    return null;
                }
            }, Integer.class));
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            // 状态的使用
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();

            Iterable<Long> longs = listState.get();
            listState.add(122L);
            listState.update(new ArrayList<>());
            listState.clear();

            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            mapState.get("");
            mapState.contains("");
            mapState.put("", 122L);
            mapState.remove("");
            mapState.clear();

            WaterSensor waterSensor = reducingState.get();
            reducingState.add(new WaterSensor());
            reducingState.clear();

            WaterSensor waterSensor1 = aggregatingState.get();
            aggregatingState.add(new WaterSensor());
            aggregatingState.clear();
        }
    }
}
