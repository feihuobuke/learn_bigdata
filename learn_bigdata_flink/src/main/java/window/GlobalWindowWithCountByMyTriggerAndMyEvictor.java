package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author: reiserx
 * Date:2020/10/31
 * Des:
 * 使用 Evictor 自己实现一个类似CountWindow(3,2)的效果
 * 每隔2个单词计算最近3个单词
 */
public class GlobalWindowWithCountByMyTriggerAndMyEvictor {

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
                //自定义 trigger
                .trigger(new MyCountTrigger(2))
                .evictor(new MyEvictor(3))
                .sum(1)
                .print();

        env.execute(GlobalWindowWithCountByMyTriggerAndMyEvictor.class.getSimpleName());
    }

    public static class MyCountTrigger extends Trigger<Tuple2<String, Long>, GlobalWindow> {
        private long maxCount;


        // 用于存储每个 key 对应的 count 值
        private ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Long.class);

        public MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        /**
         * 当一个元素进入到一个 window 中的时候就会调用这个方法
         *
         * @param element   元素
         * @param timestamp 进来的时间
         * @param window    元素所属的窗口
         * @param ctx       上下文
         * @return TriggerResult
         * 1. TriggerResult.CONTINUE ：表示对 window 不做任何处理
         * 2. TriggerResult.FIRE ：表示触发 window 的计算
         * 3. TriggerResult.PURGE ：表示清除 window 中的所有数据
         * 4. TriggerResult.FIRE_AND_PURGE ：表示先触发 window 计算，然后删除 window 中的数据
         * @throws Exception
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 拿到当前 key 对应的 count 状态值
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
            // count 累加 1
            count.add(1L);

            // 如果当前 key 的 count 值等于 maxCount
            if (count.get() == maxCount) {
                count.clear();
                //FIRE 触发 window 计算
                return TriggerResult.FIRE;
            }
            // 否则，对 window 不做任何的处理
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDescriptor).clear();
        }
    }

    private static class MyEvictor implements Evictor<Tuple2<String, Long>, GlobalWindow> {
        //window 大小
        private int windowCount;

        public MyEvictor(int windowCount) {
            this.windowCount = windowCount;
        }

        /**
         * 在 window 计算之前删除特定的数据
         *
         * @param elements       window 中所有的元素
         * @param size           window 中所有元素的大小
         * @param window         window
         * @param evictorContext 上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if (size < windowCount) {
                return;
            } else {
                int evictCount = 0;
                Iterator<TimestampedValue<Tuple2<String, Long>>> it = elements.iterator();
                while (it.hasNext()) {
                    it.next();
                    evictCount++;
                    // 如果删除的数量小于当前的 window 大小减去规定的 window 的大小，就需要删除当前的元素
                    if (size - windowCount < evictCount) {
                        break;
                    } else {
                        it.remove();
                    }
                }
            }

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

        }
    }
}
