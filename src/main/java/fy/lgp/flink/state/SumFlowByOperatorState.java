package fy.lgp.flink.state;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import fy.lgp.flink.state.function.SumFiveClearFunction;
import fy.lgp.flink.state.partitioner.StringPartitionCustom;
import fy.lgp.flink.state.source.CustomSource;

/**
 * author : li guang ping
 * description : operator State 实现每5次刷新一次单词计数
 * date : 20-5-21 下午1:08
 **/
public class SumFlowByOperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<String> source = env.addSource(new CustomSource());
        source.partitionCustom(
            new StringPartitionCustom(),
            new KeySelector<String, String>() {
                @Override
                public String getKey(String s) throws Exception {
                    return s;
                }
            }
        )
              .map(new SumFiveClearFunction())
              .print();
        env.execute("TestSum");
    }
}
