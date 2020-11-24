package com.reiser.stream.ad;

import com.reiser.stream.entity.AdClientLog;
import com.reiser.stream.entity.AdLog;
import com.reiser.stream.entity.AdServerLog;
import com.reiser.stream.untils.Constants;
import com.reiser.stream.untils.ETLUtils;
import com.reiser.stream.untils.HBaseUtils;
import com.reiser.stream.untils.KafkaProducerUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kafka.clients.producer.Producer;
import redis.clients.jedis.Jedis;

/**
 * @author: reiserx
 * Date:2020/11/20
 * Des:
 */
public class AdClientLogRichFlatMap extends RichFlatMapFunction<AdClientLog, AdLog> {
    HTable hTable;
    Jedis jedis;
    Producer producer;

    @Override
    public void open(Configuration parameters) throws Exception {
        hTable = HBaseUtils.initHbaseClient(Constants.TABLE_NAME);
//        jedis = RedisUtils.initRedis();
        producer = KafkaProducerUtils.getProducer();
        super.open(parameters);
    }

    @Override
    public void flatMap(AdClientLog adClientLog, Collector<AdLog> collector) throws Exception {
        byte[] key = ETLUtils.generateBytesKey(adClientLog);
        AdServerLog context = ETLUtils.getContext(jedis, hTable, key);
        AdLog adLog = ETLUtils.buildAdLog(adClientLog, context);

        if (context == null) {
            ETLUtils.sendRetry(producer, adLog.toByteArray());
        } else {
            collector.collect(adLog);
        }


    }
}
