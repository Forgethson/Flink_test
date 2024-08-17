package com.atguigu.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区器
 *
 * @author cjp
 * @version 1.0
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return key.hashCode() % numPartitions;
    }
}
