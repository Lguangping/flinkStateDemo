package fy.lgp.flink.state;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
