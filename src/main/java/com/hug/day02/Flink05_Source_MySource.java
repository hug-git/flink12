package com.hug.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class Flink05_Source_MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> mySourceStream = env.addSource(new MySource("hadoop102", 9999));

        mySourceStream.print();

        env.execute();
    }

    public static class MySource implements SourceFunction<String> {
        // 自定义属性信息，主机&端口号
        private String host;
        private Integer port;

        private Boolean running = true;

        Socket socket = null;
        BufferedReader reader = null;

        public MySource() {
        }

        public MySource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // 读取数据
            String line = reader.readLine();

            while (running && line != null) {
                // 接收数据并发送至Flink系统
                sourceContext.collect(line);
                line = reader.readLine();
            }
        }

        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
