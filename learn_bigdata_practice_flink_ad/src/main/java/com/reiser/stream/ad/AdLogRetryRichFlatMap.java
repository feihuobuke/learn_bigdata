package com.reiser.stream.ad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reiser.stream.entity.AdLog;
import com.reiser.stream.entity.AdServerLog;
import com.reiser.stream.entity.ProcessInfo;
import com.reiser.stream.untils.Constants;
import com.reiser.stream.untils.ETLUtils;
import com.reiser.stream.untils.HBaseUtils;
import com.reiser.stream.untils.KafkaProducerUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kafka.clients.producer.Producer;

import java.util.Iterator;

/**
 * @author: reiserx
 * Date:2020/11/18
 * Des:
 */
public class AdLogRetryRichFlatMap extends RichWindowFunction<AdLog, AdLog, String, TimeWindow> {

    String tableName = Constants.TABLE_NAME;
    HTable hTable;
//    Jedis jedis;
    Producer producer;
    ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        hTable = HBaseUtils.initHbaseClient(tableName);
//        jedis = RedisUtils.initRedis();
        producer = KafkaProducerUtils.getProducer();
        objectMapper = new ObjectMapper();
        super.open(parameters);
    }

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<AdLog> iterable, Collector<AdLog> collector) throws Exception {
        Iterator<AdLog> itr = iterable.iterator();
        while (itr.hasNext()) {
            AdLog adLog = itr.next();
            if (System.currentTimeMillis() - adLog.getProcessInfo().getProcessTimestamp() > 1000) {
                byte[] key = ETLUtils.generateBytesKey(adLog);
                // TODO 为什么不读 Hbase 了
                AdServerLog context = ETLUtils.getContext(hTable, key);
                AdLog.Builder adLogBuilder = adLog.toBuilder();
                ProcessInfo.Builder processBuilder = adLog.getProcessInfo().toBuilder();
                processBuilder.setRetryCount(processBuilder.getRetryCount() + 1);
                processBuilder.setProcessTimestamp(System.currentTimeMillis());
                adLogBuilder.setProcessInfo(processBuilder.build());
                if (context == null && adLog.getProcessInfo().getRetryCount() < 5) {
                    ETLUtils.sendRetry(producer, adLog.toByteArray());
                } else {
                    // 5 次之后依然不行，就交给离线处理了
                    collector.collect(adLog);
                }
            }
        }


    }
}
