package basic.streaming.transformation;

import basic.streaming.source.MyNoParalleSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:* 根据规则把一个数据流切分为多个流
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在 根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 */
public class StreamingBySplit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> sourceStream = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SplitStream<Long> splitStream = sourceStream.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> out = new ArrayList<>();
                if (value % 2 == 0) {
                    out.add("even");
                    out.add("even1");
                } else {
                    out.add("odd");
                }

                return out;
            }
        });

        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> evenStream2 = splitStream.select("even", "even1");
        DataStream<Long> oddStream = splitStream.select("odd");
        DataStream<Long> evenOddStream = splitStream.select("even", "odd");

        evenStream.print().setParallelism(1);


        env.execute(StreamingBySplit.class.getSimpleName());
    }
}
