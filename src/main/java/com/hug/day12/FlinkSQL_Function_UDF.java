package com.hug.day12;

import com.hug.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkSQL_Function_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(WaterSensor::toBean);

        Table table = tableEnv.fromDataStream(waterSensorDS);

        // call function "inline" without registration in Table API
        table.select(call(SubstringFunction.class, $("id"), 1, 3));

        // register function
        tableEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);

        // call registered function in Table API
        table.select(call("SubstringFunction", $("id"), 2, 3));

        // call registered function in SQL
        tableEnv.sqlQuery("SELECT SubstringFunction(id, 0, 4) FROM " + table).execute().print();

        env.execute();

    }

    public static class SubstringFunction extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }
}
