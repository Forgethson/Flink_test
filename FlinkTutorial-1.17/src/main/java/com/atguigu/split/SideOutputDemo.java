package com.atguigu.split;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流：使用侧输出流
 *
 * @author cjp
 * @version 1.0
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 3L, 3),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s4", 3L, 3)
        );

        /*
         * 需求：watersensor的数据，s1、s2的数据分别分流到不同的流中
         *  总结步骤：
         *    1、使用 process算子
         *    2、定义 OutputTag对象
         *    3、调用 ctx.output
         *    4、通过主流 获取 测流
         */

        /*
         * 创建OutputTag对象
         * 第一个参数： 标签名
         * 第二个参数： 放入侧输出流中的 数据的 类型，Typeinformation
         */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) {
                        String id = value.getId();
                        if ("s1".equals(id)) {
                            // 如果是 s1，放到侧输出流s1中
                            ctx.output(s1Tag, value);
                        } else if ("s2".equals(id)) {
                            // 如果是 s2，放到侧输出流s2中

                            ctx.output(s2Tag, value);
                        } else {
                            // 非s1、s2的数据，放到主流中
                            out.collect(value);
                        }

                    }
                }
        );
        // 从主流中，根据标签 获取 侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);
        // 打印主流
        process.print("主流（非s1、s2）");
        //打印 侧输出流
        s1.print("s1流");
        s2.print("s2流");
        env.execute();
    }
}
