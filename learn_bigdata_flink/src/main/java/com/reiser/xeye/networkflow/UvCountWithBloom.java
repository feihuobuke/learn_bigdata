package com.reiser.xeye.networkflow;

import com.reiser.xeye.product.HotProduct;
import com.reiser.xeye.product.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;

/**
 * @author: reiserx
 * Date:2020/11/7
 * Des:1 小时的 UV count
 * 大用户量使用布隆过滤器去重
 *
 */
public class UvCountWithBloom {
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
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("key", value.getUserId());
                    }
                })
                .keyBy(value -> value.f1)
                .timeWindow(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountByBloomFilter())
                .print();

        env.execute(UvCountWithBloom.class.getSimpleName());
    }

    static class Bloom {
        private int size;
        private int cap;

        Bloom(int size) {
            this.size = size;
            cap = size > 0 ? size : 1 << 27;
        }

        public long hash(String value, int seed) {
            long result = 0;
            for (int i = 0; i < value.length(); i++) {
                try {
                    result += result * seed + value.charAt(i);
                } catch (Exception e) {
                    System.out.println(value);
                    e.printStackTrace();
                }
            }
            return result & (cap - 1);
        }
    }

    private static class MyTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {
        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    private static class UvCountByBloomFilter extends ProcessWindowFunction<Tuple2<String, Long>, UvInfo, Long, TimeWindow> {

        private Bloom bloom;
        private Jedis jedis;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            bloom = new Bloom(1 << 27);
            jedis = new Jedis("localhost", 6379);
        }

        @Override
        public void process(Long key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UvInfo> out) throws Exception {
            // 位图的存储方式，key是windowEnd，value是bitmap
            //val storeKey = context.window.getEnd.toString

            String storeKey = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window().getEnd());

            long count = 0;
            if (jedis.hget("count", storeKey) != null) {
                count = Long.parseLong(jedis.hget("count", storeKey));
            }

            String userId = String.valueOf(elements.iterator().next().f1);
            long offset = bloom.hash(userId, 61);

            boolean ifExists = jedis.getbit(storeKey, offset);

            if (!ifExists) {
                jedis.setbit(storeKey, offset, true);
                jedis.hset("count", storeKey, String.valueOf(count + 1));
                out.collect(new UvInfo(context.window().getEnd(), count + 1));
            } else {
                out.collect(new UvInfo(context.window().getEnd(), count));
            }


        }
    }

    static class UvInfo {
        private long windowEnd;
        private long count;

        @Override
        public String toString() {
            return "UvInfo{" +
                    "windowEnd=" + windowEnd +
                    ", count=" + count +
                    '}';
        }

        public UvInfo(long windowEnd, long count) {
            this.windowEnd = windowEnd;
            this.count = count;
        }

        public long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}
