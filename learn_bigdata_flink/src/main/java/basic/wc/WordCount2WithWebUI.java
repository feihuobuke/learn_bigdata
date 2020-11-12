package basic.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/10/19
 * Des:有 webUI 的 word count
 * StreamExecutionEnvironment.createLocalEnvironmentWithWebUI在本地会启动一个 webUI
 * localhost:8081
 */
public class WordCount2WithWebUI {
    public static void main(String[] args) throws Exception {
        //步骤一：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //步骤二：获取数据源
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        //步骤三：数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }

        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordAndOne.keyBy(0).sum(1);
        wordCount.print();
        //步骤四：数据输出 wordCount.print(); //步骤五：启动任务
        env.execute("word count ...");

    }
}
