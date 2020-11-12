package basic.streaming.sink;

import basic.streaming.source.MyNoParalleSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:* sink->WriteText
 */
public class StreamingBySink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> sourceStream = env.addSource(new MyNoParalleSource()).setParallelism(1);
        sourceStream.filter(s -> {
            return s % 2 == 0;
        }).writeAsText("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_flink/src/main/resources/test").setParallelism(4);

        env.execute(StreamingBySink.class.getSimpleName());
    }
}
