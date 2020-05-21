package fy.lgp.flink.state.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;

/**
 * author : Li Guang Ping
 * description : 字符串类型的自定义分区器
 * date : 20-1-22 上午11:44
 **/
public class StringPartitionCustom implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        // 原keyBy的分区计算方式
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
            key,
            StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
            numPartitions
        );
    }
}
