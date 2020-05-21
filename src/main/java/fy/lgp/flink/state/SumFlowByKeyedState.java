package fy.lgp.flink.state;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import fy.lgp.flink.state.function.SumFiveClearFunctionByValue;
import fy.lgp.flink.state.source.CustomSource;

/**
 * author : li guang ping
 * description : keyed State 实现每5次刷新一次单词计数
 * date : 20-5-21 下午1:08
 **/
public class SumFlowByKeyedState {


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new RuntimeException("hdfs检查点目录未配置");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 检查点设置 每20秒启动一个检查点 , 每个检查点启动之间必须间隔10秒 , 检查点必须在2分钟内完成，或者被丢弃
        env.enableCheckpointing(1000 * 10, CheckpointingMode.AT_LEAST_ONCE);
        env.setParallelism(2);
        env.setStateBackend(new RocksDBStateBackend(args[0]));
        // 作业取消时不删除的外部检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> source = env.addSource(new CustomSource());
        source
            .keyBy(new KeySelector<String, String>() {
                @Override
                public String getKey(String s) throws Exception {
                    return s;
                }
            })
//            .map(new SumFiveClearFunctionByReduce())
            .map(new SumFiveClearFunctionByValue())
            .print();
        env.execute("TestSum");
    }


}
