package com.atguigu.process;

import com.atguigu.bean.WaterSensor;
import com.atguigu.utils.DataSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;

/**
 * KeyedProcessFunctionTopNDemo的优化版本：需要区分数据流的id
 *
 * @author cjp
 * @version 1.0
 */
public class KeyedProcessFunctionTopNDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = DataSourceUtil.getWaterSensorDataStreamSource(env);
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, ts) -> element.getTs())
        );

        // 1. 按照 vc 分组、开窗（10s滑动窗口，步长为5s）、聚合（增量计算+全量打标签）
        //  开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口的标记 windowEnd
        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Long>> windowAgg = sensorDSwithWatermark.keyBy(
                        new KeySelector<WaterSensor, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> getKey(WaterSensor ws) {
                                return Tuple2.of(ws.getId(), ws.getVc());
                            }
                        })
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new VcCountAgg(),
                        new WindowResult()
                );
        // 此时windowAgg的output是4元组Tuple4<String, Integer, Integer, Long> (id, vc, count, windowEnd)
//        windowAgg.print("Tuple4(id, vc, count, windowEnd)= ");

        // 按照id进行分流
        List<String> ids = Arrays.asList("s1", "s2", "s3");
        HashMap<String, OutputTag<Tuple3<Integer, Integer, Long>>> tagMap = new HashMap<>();
        for (String id : ids) {
            tagMap.put(id, new OutputTag<>(id, Types.TUPLE(Types.INT, Types.INT, Types.LONG)));
        }

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> process = windowAgg.process(
                new ProcessFunction<Tuple4<String, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>() {
                    @Override
                    public void processElement(Tuple4<String, Integer, Integer, Long> value,
                                               ProcessFunction<Tuple4<String, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>.Context ctx,
                                               Collector<Tuple3<Integer, Integer, Long>> out) {
                        String id = value.f0;
                        if ("s1".equals(id)) {
                            ctx.output(tagMap.get("s1"), Tuple3.of(value.f1, value.f2, value.f3));
                        } else if ("s2".equals(id)) {
                            ctx.output(tagMap.get("s2"), Tuple3.of(value.f1, value.f2, value.f3));
                        } else if ("s3".equals(id)) {
                            ctx.output(tagMap.get("s3"), Tuple3.of(value.f1, value.f2, value.f3));
                        } else {
                            // 放到主流中
                            out.collect(Tuple3.of(value.f1, value.f2, value.f3));
                        }
                    }
                });

        // 2. 按照id分流后，对每个id的流，进行排序、取前N个
        for (String id : ids) {
            process.getSideOutput(tagMap.get(id))
                    .keyBy(r -> r.f2)
                    .process(new TopN(2))
                    .print(id + "流");
        }
        env.execute();
    }


    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


    /**
     * 泛型如下：
     * 第一个：输入类型 = 增量函数的输出 count值，Integer
     * 第二个：输出类型 = Tuple4(id, vc, count, windowEnd) ,带上 窗口结束时间 的标签
     * 第三个：key类型 = Tuple2(id, vc)
     * 第四个：窗口类型
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple4<String, Integer, Integer, Long>, Tuple2<String, Integer>, TimeWindow> {
        @Override
        public void process(Tuple2<String, Integer> key, Context context, Iterable<Integer> elements, Collector<Tuple4<String, Integer, Integer, Long>> out) throws Exception {
            // 迭代器里面只有一条数据，next一次即可
            Integer count = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            out.collect(Tuple4.of(key.f0, key.f1, count, windowEnd));
        }
    }


    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        // 存不同窗口的 统计结果，key=windowEnd，value=list数据
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        // 要取的Top数量
        private int N;

        public TopN(int N) {
            this.N = N;
            dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法，只是一条数据，要排序，得到齐才行 ===》 存起来，不同窗口分开存
            // 1. 存到HashMap中
            Long windowEnd = value.f2;
            if (dataListMap.containsKey(windowEnd)) {
                // 1.1 包含vc，不是该vc的第一条，直接添加到List中
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(value);
            } else {
                // 1.1 不包含vc，是该vc的第一条，需要初始化list
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataListMap.put(windowEnd, dataList);
            }

            // 2. 注册一个定时器， windowEnd+1ms即可（
            // 同一个窗口范围，应该同时输出，只不过是一条一条调用processElement方法，只需要延迟1ms即可
            ctx.timerService().registerEventTimeTimer(windowEnd + 1);

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 定时器触发，同一个窗口范围的计算结果攒齐了，开始 排序、取TopN
            Long windowEnd = ctx.getCurrentKey();
            // 1. 排序
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    // 降序， 后 减 前
                    return o2.f1 - o1.f1;
                }
            });
            // 2. 取TopN
            StringBuilder outStr = new StringBuilder();
            outStr.append("================================\n");
            // 遍历 排序后的 List，取出前 threshold 个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
            for (int i = 0; i < Math.min(N, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top").append(i + 1).append("|")
                        .append("vc=").append(vcCount.f0).append("|")
                        .append("count=").append(vcCount.f1).append("|")
                        .append("窗口结束时间=").append(vcCount.f2).append("|")
                        .append("================================\n");
            }
            // 用完的List，及时清理，节省资源
            dataList.clear();
            out.collect(outStr.toString());
        }
    }
}
