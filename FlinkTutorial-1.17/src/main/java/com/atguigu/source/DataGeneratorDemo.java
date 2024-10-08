package com.atguigu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cjp
 * @version 1.0
 */
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         *   如果有n个并行度， 最大值设为a
         *   将数值 均分成 n份，  a/n ,比如，最大100，并行度2，每个并行度生成50个
         *   其中一个是 0-49，另一个50-99
         */
        env.setParallelism(2);
        /*
         * 数据生成器Source，四个参数：
         *     第一个 generatorFunction：GeneratorFunction接口，需要实现，重写map方法，输入类型固定是Long
         *     第二个 count： long类型，自动生成的数字序列（从0自增）的最大值，达到这个值就重置了
         *     第三个 rateLimiterStrategy：限速策略，比如每秒生成几条数据
         *     第四个 typeInfo：返回的类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                100,
                RateLimiterStrategy.perSecond(4),
                Types.STRING
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator").print("data-generator");
        env.execute();
    }
}
