package com.hug.day03;

import com.hug.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink10_Sink_MySink {
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

        waterSensorDS.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<WaterSensor> {
        private PreparedStatement ps;
        private Connection conn;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "000000");
            ps = conn.prepareStatement("insert into sensor values(?, ?, ?)" +
                    "on duplicate key update `ts`=?, `vc`=?");
        }

        @Override
        public void close() throws Exception {
            ps.close();
            conn.close();
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            ps.setLong(4, value.getTs());
            ps.setInt(5, value.getVc());
            ps.execute();
        }
    }
}
