package basic.streaming.transformation;

import basic.streaming.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:算子练习：Connect
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理 方法
 */
public class StreamingByConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<String> strSource2 = source2.map((MapFunction<Long, String>) v -> "str_" + v);

        ConnectedStreams<Long, String> connectStream = source1.connect(strSource2);

        SingleOutputStreamOperator<Object> mapStream = connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        mapStream.print().setParallelism(1);

        env.execute(StreamingByConnect.class.getSimpleName());
    }
}
