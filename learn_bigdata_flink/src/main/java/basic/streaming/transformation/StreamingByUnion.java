package basic.streaming.transformation;

import basic.streaming.source.MyNoParalleSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:算子练习：Union
 * 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致 的
 */
public class StreamingByUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStream<Long> textStream = source1.union(source2);

        SingleOutputStreamOperator<Long> unionStream = textStream.map(value -> {
            System.out.println("收到原始数据：" + value);
            return value;
        });

        SingleOutputStreamOperator<Long> sumStream = unionStream.timeWindowAll(Time.seconds(2)).sum(0);
        sumStream.print().setParallelism(1);

        env.execute(StreamingByUnion.class.getSimpleName());
    }
}
