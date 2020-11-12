package watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * @author: reiserx
 * Date:2020/10/31
 * Des:使用 EventTime + Watermark 解决乱序问题
 */
public class StreamWithNoOrderByEventTimeAndWatetmark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.addSource(new TestSource());

        //步骤一：设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], Long.valueOf(fields[1]));
            }
        })
                //步骤二：获取数据里面的event Time
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new SumProcessWindowFunction());

        result.print().setParallelism(1);

        env.execute("WaterMarkWithNoOrder");
    }

    private static class TestSource implements SourceFunction<String> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String currTime = String.valueOf(System.currentTimeMillis());

            while (Integer.parseInt(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
            }

            System.out.println("开始发送事件的时间：" + dateFormat.format(System.currentTimeMillis()));

            TimeUnit.SECONDS.sleep(13);
            ctx.collect("hadoop," + System.currentTimeMillis());
            String lateStr = "hadoop," + System.currentTimeMillis();
            // 因为延迟而没有发送 lateStr
            // ctx.collect(lateStr);
            TimeUnit.SECONDS.sleep(3);
            ctx.collect("hadoop," + System.currentTimeMillis());
            TimeUnit.SECONDS.sleep(3);
            /*
             * 本该在 13s 发送的 lateStr，因为数据延迟第 19s 才发送
             *
             * 正确的顺序是
             * (hadoop,2)
             * (hadoop,3)
             * (hadoop,1)
             *
             * 乱序之后变成
             * (hadoop,1)
             * (hadoop,3)
             * (hadoop,2)
             */
            ctx.collect(lateStr);
            TimeUnit.SECONDS.sleep(300);

        }

        @Override

        public void cancel() {

        }
    }

    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型（在Flink里面，String用Tuple表示）
     * W：Window的数据类型
     */
    private static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>, Tuple, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        /**
         * 当一个window触发计算的时候会调用这个方法
         *
         * @param tuple    key
         * @param context  operator的上下文
         * @param elements 指定window的所有元素
         * @param out      用户输出
         */

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            int sum = 0;

            for (Tuple2<String, Long> element : elements) {
                sum++;
            }

            out.collect(Tuple2.of(tuple.getField(0), sum));
        }
    }


    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 使用 Watermark 解决乱序
            return new Watermark(System.currentTimeMillis() - 5000);
        }


        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            //使用 event time 作为处理事件，解决乱序
            return element.f1;
        }
    }
}

