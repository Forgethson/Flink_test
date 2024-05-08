package com.atguigu.utils;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class DataSourceUtil {
    private static final Random random = new Random();

    public static DataStreamSource<WaterSensor> getWaterSensorDataStreamSource(StreamExecutionEnvironment env) {
        List<String> keys = Arrays.asList("s1", "s2", "s3");
        DataGeneratorSource<WaterSensor> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, WaterSensor>() {
                    @Override
                    public WaterSensor map(Long value) {
                        return new WaterSensor(keys.get(random.nextInt(keys.size())), System.currentTimeMillis(), value.intValue());
                    }
                },
                1000L,
                RateLimiterStrategy.perSecond(3),
                Types.POJO(WaterSensor.class)
        );
        DataStreamSource<WaterSensor> sensorDS = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        return sensorDS;
    }

    public static DataStreamSource<Integer> getNumberDataStreamSource(StreamExecutionEnvironment env) {
        DataGeneratorSource<Integer> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, Integer>() {
                    @Override
                    public Integer map(Long value) {
//                        if (value % 2 == 0) {
//                            return value.intValue() + 1;
//                        }
                        return value.intValue();
                    }
                },
                10000L,
                RateLimiterStrategy.perSecond(3),
                Types.INT
        );
        DataStreamSource<Integer> numberDS = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "number-data-generator");
        return numberDS;
    }
}
