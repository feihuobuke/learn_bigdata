package com.reiser.xeye.networkflow;

import com.reiser.xeye.product.HotProduct;
import com.reiser.xeye.product.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author: reiserx
 * Date:2020/11/7
 * Des:1 小时的 UV count
 * 小用户量使用 set 去重
 */
public class UvCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //注册时间类型为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data1.csv").map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new HotProduct.ProductEventTimeExtractor())
                .filter(value -> "pv".equals(value.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountByWindow())
                .print();

        env.execute(UvCount.class.getSimpleName());

    }

    static class UvCountByWindow implements AllWindowFunction<UserBehavior, UvInfo, TimeWindow> {


        @Override
        public void apply(TimeWindow window, java.lang.Iterable<UserBehavior> values, Collector<UvInfo> out) throws Exception {
            HashSet<Long> set = new HashSet<>();

            for (UserBehavior item : values) {
                set.add(item.getUserId());
            }
            out.collect(new UvInfo(window.getEnd(), set.size()));
        }
    }

    static class UvInfo {
        private long windowEnd;
        private int count;

        @Override
        public String toString() {
            return "UvInfo{" +
                    "windowEnd=" + windowEnd +
                    ", count=" + count +
                    '}';
        }

        public UvInfo(long windowEnd, int count) {
            this.windowEnd = windowEnd;
            this.count = count;
        }

        public long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
