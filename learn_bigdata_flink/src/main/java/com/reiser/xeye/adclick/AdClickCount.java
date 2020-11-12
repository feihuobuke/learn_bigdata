package com.reiser.xeye.adclick;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @author: reiserx
 * Date:2020/11/8
 * Des:广告点击统计
 * 1.一天之内点击超过100次的 进入黑名单
 * 2.每隔5秒统计最近1小时的各省份的广告点击
 */
public class AdClickCount {

    static OutputTag<BlackListWarning> mBlackListWarningOutputTag = new OutputTag<BlackListWarning>("blackwaring") {
    };


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //注册时间类型为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //1.获取数据
        SingleOutputStreamOperator<AdClickEvent> sourceStream = env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data3.csv").map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String line) throws Exception {
                String[] fields = line.split(",");
                return new AdClickEvent(Long.parseLong(fields[0]), Long.parseLong(fields[1]), fields[2], fields[3], Long.parseLong(fields[4]));
            }
        }).assignTimestampsAndWatermarks(new AdClickEventTimeExtractor());
        //计算黑名单
        SingleOutputStreamOperator<AdClickEvent> filterBlackStream = sourceStream.keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickEvent value) throws Exception {
                return Tuple2.of(value.getUserId(), value.getAdId());
            }
        }).process(new FilterBlackListUser(100));

        filterBlackStream.getSideOutput(mBlackListWarningOutputTag).print();

        // 说去各省的广告点击
        filterBlackStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new AdClickEventCount(), new AdClickWindow())
                .print();

        env.execute(AdClickCount.class.getSimpleName());

    }

    static class AdClickWindow implements WindowFunction<Long, CountByProvince, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<CountByProvince> out) throws Exception {
            out.collect(new CountByProvince(String.valueOf(window.getEnd()), key, input.iterator().next()));


        }
    }

    static class AdClickEventCount implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    static class AdClickEventTimeExtractor implements AssignerWithPeriodicWatermarks<AdClickEvent> {
        long maxEventTimestamps = 0;
        long maxOutputOrderness = 10000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxEventTimestamps - maxOutputOrderness);
        }

        @Override
        public long extractTimestamp(AdClickEvent element, long previousElementTimestamp) {
            long currentEventTimestamps = element.getTimestamp() * 1000;
            maxEventTimestamps = Math.max(currentEventTimestamps, maxEventTimestamps);
            return currentEventTimestamps;
        }
    }


    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {
        private ValueState<Long> mCountState;
        private ValueState<Long> mTimeState;
        private ValueState<Boolean> mInBlackListState;
        private int mMaxCount;

        FilterBlackListUser(int maxCount) {
            this.mMaxCount = maxCount;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            mCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Long.class));
            mTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("time-state", Long.class));
            mInBlackListState = getRuntimeContext().getState(new ValueStateDescriptor<>("black-state", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {


            Long count = mCountState.value();
            if (count == null) {
                long time = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24);
                mTimeState.update(time);
                ctx.timerService().registerEventTimeTimer(time);
                count = 0L;
            }

            count++;
            if (count >= mMaxCount) {
                if(mInBlackListState.value() ==null) {
                    mInBlackListState.update(true);
                    ctx.output(mBlackListWarningOutputTag, new BlackListWarning(value.getUserId(), value.getAdId(), "Click over" + mMaxCount + " times"));
                }
                return;
            }
            mCountState.update(count);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (timestamp == mTimeState.value()) {
                mTimeState.clear();
                mCountState.clear();
            }
        }
    }


    static class AdClickEvent {
        private Long userId;
        private Long adId;
        private String province;
        private String city;
        private Long timestamp;

        public AdClickEvent(Long userId, Long adId, String province, String city, Long timestamp) {
            this.userId = userId;
            this.adId = adId;
            this.province = province;
            this.city = city;
            this.timestamp = timestamp;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getAdId() {
            return adId;
        }

        public void setAdId(Long adId) {
            this.adId = adId;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }

    static class CountByProvince {
        private String windowEnd;
        private String province;
        private Long count;

        public CountByProvince(String windowEnd, String province, Long count) {
            this.windowEnd = windowEnd;
            this.province = province;
            this.count = count;
        }

        public String getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(String windowEnd) {
            this.windowEnd = windowEnd;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "CountByProvince{" +
                    "windowEnd='" + windowEnd + '\'' +
                    ", province='" + province + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    public static class BlackListWarning {
        private Long userId;
        private Long adId;
        private String msg;

        public BlackListWarning(Long userId, Long adId, String msg) {
            this.userId = userId;
            this.adId = adId;
            this.msg = msg;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getAdId() {
            return adId;
        }

        public void setAdId(Long adId) {
            this.adId = adId;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "BlackListWarning{" +
                    "userId=" + userId +
                    ", adId=" + adId +
                    ", msg='" + msg + '\'' +
                    '}';
        }
    }
}
