package com.reiser.xeye.networkflow;

import com.reiser.xeye.product.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @author: reiserx
 * Date:2020/11/7
 * Des: 计算每小时的 pv
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data1.csv").map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new PageViewEventTimeExtractor())
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                //不是使用 lamda
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                        return Tuple2.of("pv", 1);
                    }
                })
                .timeWindowAll(Time.hours(1))
                .sum(1)
                .print();

        env.execute(PageView.class.getSimpleName());

    }

    /**
     * 设置 watermark
     */
    static class PageViewEventTimeExtractor implements AssignerWithPeriodicWatermarks<UserBehavior> {

        Long currentMaxEventTime = 0L;
        Long maxOutputOrderness = 10000L;


        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime * 1000 - maxOutputOrderness);
        }

        @Override
        public long extractTimestamp(UserBehavior element, long previousElementTimestamp) {
            Long currentEventTime = element.getTimestamp();
            currentMaxEventTime = Math.max(currentMaxEventTime, currentEventTime);
            return currentEventTime * 1000;
        }
    }
}
