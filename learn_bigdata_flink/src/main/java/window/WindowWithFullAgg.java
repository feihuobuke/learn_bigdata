package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author: reiserx
 * Date:2020/11/3
 * Des:全量聚合
 * 等属于窗⼝的数据到⻬齐，才开始进⾏聚合计算【可以实现对窗⼝内的数据进⾏排序等需求】
 * 常用算子 apply(windowFunction) process(processWindowFunction) processWindowFunction⽐windowFunction提供了更多的上下⽂信息。类似于map和RichMap的关系
 */
public class WindowWithFullAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 8888);
        sourceStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        }).timeWindowAll(Time.seconds(10)).process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                System.out.println("开始执行逻辑运算");
                //Collections.sort();
                int count = 0;
                Iterator<Integer> it = elements.iterator();
                while (it.hasNext()) {
                    Integer number = it.next();
                    count += number;

                }
                out.collect(count);

            }
        }).print();

        env.execute(WindowWithIncrAgg.class.getSimpleName());
    }
}
