package com.reiser.xeye.etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ScanResult;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: reiserx
 * Date:2020/11/8
 * Des:real time ETL
 */
public class RealTimeEtl {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final MapStateDescriptor<String, String> broadcastDes = new MapStateDescriptor<>(
                "broadcast",
                String.class,
                String.class);
        BroadcastStream<String> broadcast = env.addSource(new BroadcastSourceFunction()).broadcast(broadcastDes);

        env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data5.log")
                .connect(broadcast).process(new BroadcastProcessFunction<String, String, RealTimeEvent>() {

            @Override
            public void processElement(String line, ReadOnlyContext ctx, Collector<RealTimeEvent> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastDes);
                String broadcastValue = broadcastState.get("broadcastState");
                HashMap<String, String> countryMap = JSONObject.parseObject(broadcastValue, HashMap.class);
                LogEvent value = JSON.parseObject(line, LogEvent.class);
                if (value != null && value.getData() != null) {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    for (LogEvent.DataBean item : value.getData()) {
                        RealTimeEvent realTimeEvent = new RealTimeEvent(value.getDt()
                                , dateFormat.parse(value.getDt()).getTime()
                                , value.getCountryCode(), item.getType()
                                , item.getScore()
                                , item.getLevel());
                        if (countryMap != null) {
                            realTimeEvent.setArea(countryMap.getOrDefault(realTimeEvent.getCountryCode(), ""));
                        }
                        out.collect(realTimeEvent);
                    }
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<RealTimeEvent> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastDes);
                broadcastState.put("broadcastState", value);
            }


        }).print();

        env.execute(RealTimeEtl.class.getSimpleName());

    }


    static class BroadcastSourceFunction extends RichSourceFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            Jedis jedisSubscriber = new Jedis("localhost", 6379);
            Jedis jedisClient = new Jedis("localhost", 6379);

            sendNewestMap(sourceContext, jedisClient);
            // 使用 psubscribe 实时监控数据变化
            jedisSubscriber.psubscribe(new JedisPubSub() {
                /**
                 * 取得按表达式的方式订阅的消息后的处理
                 */
                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    super.onPMessage(pattern, channel, message);
                    sendNewestMap(sourceContext, jedisClient);
                }


                /**
                 * 取得订阅的消息后的处理
                 */
                @Override
                public void onMessage(String channel, String message) {
                    super.onMessage(channel, message);
                }
            }, "__keyspace@0__:areas");

            jedisClient.close();
            jedisSubscriber.close();

        }

        private void sendNewestMap(SourceContext<String> sourceContext, Jedis jedisClient) {
            ScanResult<Map.Entry<String, String>> areas = jedisClient.hscan("areas", "0");
            List<Map.Entry<String, String>> result = areas.getResult();
            HashMap<String, String> hashMap = new HashMap<>();

            for (Map.Entry<String, String> stringStringEntry : result) {
                String key = stringStringEntry.getKey();
                String[] split = stringStringEntry.getValue().split(",");
                for (String s : split) {
                    hashMap.put(s, key);

                }
            }
            sourceContext.collect(JSON.toJSONString(hashMap));
        }

        @Override
        public void cancel() {
        }
    }
}
