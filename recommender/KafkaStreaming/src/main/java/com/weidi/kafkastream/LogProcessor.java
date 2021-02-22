package com.weidi.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        // 把收集到的日志信息用string表示
        String line = new String(value);
        // 根据前缀MOVIE_RATING_PREFIX:从日志信息中提取评分数据
        if (line.contains("MOVIE_RATING_PREFIX:")) {
            System.out.println("movie rating data coming!>>>>>>>>>>>>>>>>>");

            line = line.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), line.getBytes());
        }

    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
