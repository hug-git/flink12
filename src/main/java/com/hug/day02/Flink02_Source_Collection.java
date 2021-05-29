package com.hug.day02;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<WaterSensor> waterSensors = Arrays.asList(new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);

        waterSensorDataStreamSource.print();

        env.execute();
    }
}
