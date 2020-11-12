package com.reiser.xeye.product;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * @author: reiserx
 * Date:2020/11/6
 * Des: 每 5 分钟统计一小时的热门商品 top 5
 * 读取数据
 * 过滤用户行为 读取数据
 * 按商品分组
 * 统计窗口数据
 * 根据窗口分组
 * 商品TopN排序
 * 添加水位
 * 打印输出
 */

public class HotProduct {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //注册时间类型为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data1.csv").map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new ProductEventTimeExtractor())
                .filter(value -> "pv".equals(value.getBehavior()))
                .keyBy(UserBehavior::getProductId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ProductCount(), new WindowResult())
                .keyBy(ProductViewCount::getWindowEnd)
                .process(new TopHotProduce(5))
                .print();

        env.execute(HotProduct.class.getSimpleName());

    }

    /**
     * 使用 ListState 存储 ProductViewCount，并在窗口结束时触发排序操作，得到 top 5
     */
    static class TopHotProduce extends KeyedProcessFunction<Long, ProductViewCount, String> {
        private int n;
        private ListState<ProductViewCount> producetState;

        TopHotProduce(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            producetState = getRuntimeContext().getListState(new ListStateDescriptor<>("url-state", ProductViewCount.class));

        }

        @Override
        public void processElement(ProductViewCount value, Context ctx, Collector<String> out) throws Exception {
            producetState.add(value);
            //TODO 重复注册了吗？
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ProductViewCount> it = producetState.get().iterator();
            PriorityQueue<ProductViewCount> maxQueue = new PriorityQueue<>(new Comparator<ProductViewCount>() {
                @Override
                public int compare(ProductViewCount o1, ProductViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });
            while (it.hasNext()) {
                maxQueue.add(it.next());
            }

            //把结果拼接成为字符串，输出去
            StringBuilder sb = new StringBuilder();
            sb.append("时间：").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp - 1)).append("\n");

            for (int i = 0; i < n && maxQueue.size() > 0; i++) {
                ProductViewCount product = maxQueue.poll();
                sb.append(" 商品ID：").append(product.getProductId())
                        .append(" 商品浏览量=").append(product.getCount())
                        .append("\n");

            }

            //清空 state
            producetState.clear();
            sb.append("============================");
            out.collect(sb.toString());


        }
    }

    /**
     * 将window的结果输出为 ProductViewCount
     */
    static class WindowResult implements WindowFunction<Long, ProductViewCount, Long, TimeWindow> {


        @Override
        public void apply(Long key, TimeWindow window, java.lang.Iterable<Long> input, Collector<ProductViewCount> out) throws Exception {
            if (input.iterator().hasNext()) {
                out.collect(new ProductViewCount(key, window.getEnd(), input.iterator().next()));
            }
        }
    }

    /**
     * 计算商品的 pv 的 count
     */
    static class ProductCount implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {

            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 设置 watermark
     */
    public static class ProductEventTimeExtractor implements AssignerWithPeriodicWatermarks<UserBehavior> {

        Long currentMaxEventTime = 0L;
        Long maxOutputOrderness = 10000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutputOrderness);
        }

        @Override
        public long extractTimestamp(UserBehavior userBehavior, long previousElementTimestamp) {
            long currentElementEventTime = userBehavior.getTimestamp() * 1000;
            currentMaxEventTime = Math.max(currentElementEventTime, currentMaxEventTime);
            return currentElementEventTime;
        }
    }


}



