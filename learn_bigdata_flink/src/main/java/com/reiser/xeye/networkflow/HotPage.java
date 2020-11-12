package com.reiser.xeye.networkflow;

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
 * Des: 每 5 秒统计10分钟的热门网页 top 5
 */

public class HotPage {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //注册时间类型为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data2.log").map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            return new ApachePageEvent(fields[0].trim(), fields[1].trim(), dateFormat.parse(fields[3].trim()).getTime(), fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new UrlEventTimeExtractor())
                .keyBy(ApachePageEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new ProductCount(), new WindowResult())
                .keyBy(UrlViewCount::getWindowEnd)
                .process(new TopHotUrl(5))
                .print();

        env.execute(HotPage.class.getSimpleName());

    }

    /**
     * 使用 ListState 存储 ProductViewCount，并在窗口结束时触发排序操作，得到 top 5
     */
    static class TopHotUrl extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private int n;
        private ListState<UrlViewCount> producetState;

        TopHotUrl(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            producetState = getRuntimeContext().getListState(new ListStateDescriptor<>("product-state", UrlViewCount.class));

        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            producetState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<UrlViewCount> it = producetState.get().iterator();
            PriorityQueue<UrlViewCount> maxQueue = new PriorityQueue<>(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
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
                UrlViewCount product = maxQueue.poll();
                sb.append(" URL:").append(product.getUrl())
                        .append(" 访问量=").append(product.getCount())
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
    static class WindowResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {


        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            if (input.iterator().hasNext()) {
                out.collect(new UrlViewCount(url, window.getEnd(), input.iterator().next()));
            }
        }
    }

    /**
     * 计算商品的 pv 的 count
     */
    static class ProductCount implements AggregateFunction<ApachePageEvent, Long, Long> {

        @Override
        public Long createAccumulator() {

            return 0L;
        }

        @Override
        public Long add(ApachePageEvent value, Long accumulator) {
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
    static class UrlEventTimeExtractor implements AssignerWithPeriodicWatermarks<ApachePageEvent> {

        Long currentMaxEventTime = 0L;
        Long maxOutputOrderness = 10000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutputOrderness);
        }

        @Override
        public long extractTimestamp(ApachePageEvent evet, long previousElementTimestamp) {
            long currentElementEventTime = evet.getEventTime();
            currentMaxEventTime = Math.max(currentElementEventTime, currentMaxEventTime);
            return currentElementEventTime;
        }
    }


    static class ApachePageEvent {
        private String ip;
        private String userId;
        private Long eventTime;
        private String method;
        private String url;

        public ApachePageEvent(String ip, String userId, Long eventTime, String method, String url) {
            this.ip = ip;
            this.userId = userId;
            this.eventTime = eventTime;
            this.method = method;
            this.url = url;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Long getEventTime() {
            return eventTime;
        }

        public void setEventTime(Long eventTime) {
            this.eventTime = eventTime;
        }

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
            this.method = method;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }


    static class UrlViewCount {
        private String url;
        private Long windowEnd;
        private Long count;

        public UrlViewCount(String url, Long windowEnd, Long count) {
            this.url = url;
            this.windowEnd = windowEnd;
            this.count = count;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public Long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(Long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }


}



