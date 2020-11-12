package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/10/31
 * Des:global window + trigger ⼀起配合才能使⽤
 * 需求：单词每出现三次统计⼀次
 */
public class GlobalWindowWithCountByTrigger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordoneStream = sourceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {

                String[] words = line.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });


        wordoneStream.keyBy(0)
                .window(GlobalWindows.create())
                //flink 自带
                .trigger(CountTrigger.of(3))
                .sum(1)
                .print();

        env.execute(GlobalWindowWithCountByTrigger.class.getSimpleName());
    }
}
