package com.reiser.stream.ad;

import com.reiser.stream.entity.AdServerLog;
import com.reiser.stream.untils.Constants;
import com.reiser.stream.untils.ETLUtils;
import com.reiser.stream.untils.HBaseUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.HTable;
import redis.clients.jedis.Jedis;

/**
 * @author: reiserx
 * Date:2020/11/20
 * Des:
 */
public class AdServerLogRichFlatMap extends RichFlatMapFunction<AdServerLog, String> {
    HTable hTable;
    Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {

        hTable = HBaseUtils.initHbaseClient(Constants.TABLE_NAME);
//        jedis = RedisUtils.initRedis();
        super.open(parameters);
    }

    @Override
    public void flatMap(AdServerLog adServerLog, Collector<String> collector) throws Exception {
        byte[] key = ETLUtils.generateBytesKey(adServerLog);
        AdServerLog context = ETLUtils.generateContext(adServerLog);
        ETLUtils.writeRedis(jedis, key, context);
        ETLUtils.writeHbase(hTable, key, context);

    }
}
