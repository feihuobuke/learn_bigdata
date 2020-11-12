package basic.streaming.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:算子练习：flatMap
 */
public class StreamingByWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineStream = env.socketTextStream("localhost", 8888, "\n", 3);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordStream = lineStream.flatMap((s, collector) -> {
            for (String word : s.split(",")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = wordStream.keyBy(0)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);

        resultStream.print().setParallelism(1);

        env.execute("socket word count");
    }
}
