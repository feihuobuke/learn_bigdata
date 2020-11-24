package com.reiser.stream.untils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.reiser.stream.entity.AdClientLog;
import com.reiser.stream.entity.AdLog;
import com.reiser.stream.entity.AdServerLog;
import com.reiser.stream.entity.ProcessInfo;
import com.reiser.stream.entity.dto.AdLogDTO;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2020/11/17
 * Des:
 */
public class ETLUtils {
    public static byte[] generateBytesKey(AdClientLog context) {
        String stringBuilder = context.getRequestId() +
                context.getCreativeId() +
                context.getUnitId();
        return stringBuilder.getBytes();
    }

    public static byte[] generateBytesKey(AdLog context) {
        String stringBuilder = context.getRequestId() +
                context.getCreativeId() +
                context.getUnitId();
        return stringBuilder.getBytes();
    }

    public static byte[] generateBytesKey(AdServerLog context) {
        String stringBuilder = context.getRequestId() +
                context.getCreativeId() +
                context.getUnitId();
        return stringBuilder.getBytes();
    }

    public static AdServerLog generateContext(AdServerLog adServerLog) {
        AdServerLog.Builder context = AdServerLog.newBuilder();
        context.setGender(adServerLog.getGender());
        context.setAge(adServerLog.getAge());
        context.setCountry(adServerLog.getCountry());
        context.setSourceType(adServerLog.getSourceType());
        context.setBidType(adServerLog.getBidType());
        return context.build();
    }

    static final int DEFAULT_EXPIRE = 1 * 60 * 60;

    static LRUCache cache = new LRUCache(DEFAULT_EXPIRE);

    public static void writeRedis(Jedis jedis, byte[] key, AdServerLog context) {
        cache.put(key, context.toByteArray());
//        jedis.set(key, context.toByteArray());
//        jedis.expire(key, DEFAULT_EXPIRE);
    }

    static final byte[] DEFAULT_COLUMN_FAMILY = Bytes.toBytes("cf1");
    static final byte[] DEFAULT_COLUMN_KEY = Bytes.toBytes("v");

    public static void writeHbase(HTable hTable, byte[] key, AdServerLog context) {
        try {
            Put put = new Put(key);
            put.add(DEFAULT_COLUMN_FAMILY, DEFAULT_COLUMN_KEY, context.toByteArray());
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static AdServerLog getContext(Jedis jedis, HTable hTable, byte[] key) {
        AdServerLog context = getContext(jedis, key);
        if (context == null)
            context = getContext(hTable, key);
        return context;
    }

    public static AdServerLog getContext(Jedis jedis, byte[] key) {
//        byte[] data = jedis.get(key);
        byte[] data = cache.get(key);
        if (data == null)
            return null;
        try {
            return AdServerLog.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static AdServerLog getContext(HTable hTable, byte[] key) {
        Get get = new Get(key);
        try {
            Result data = hTable.get(get);
            if (data == null)
                return null;
            return AdServerLog.parseFrom(data.getValue(DEFAULT_COLUMN_FAMILY, DEFAULT_COLUMN_KEY));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static AdLog buildAdLog(AdClientLog adClientLog, AdServerLog context) {
        AdLog.Builder adLogBuilder = AdLog.newBuilder();
        //TODO processInfoBuilder 没用啊
        ProcessInfo.Builder processInfoBuilder = ProcessInfo.newBuilder();
        processInfoBuilder.setProcessTimestamp(System.currentTimeMillis());

        adLogBuilder.setRequestId(adClientLog.getRequestId());
        adLogBuilder.setTimestamp(adClientLog.getTimestamp());
        adLogBuilder.setDeviceId(adClientLog.getDeviceId());
        adLogBuilder.setOs(adClientLog.getOs());
        adLogBuilder.setNetwork(adClientLog.getNetwork());
        adLogBuilder.setUserId(adClientLog.getUserId());
        if (context != null) {
            processInfoBuilder.setJoinServerLog(true);
            joinContext(adLogBuilder, context);
        } else {
            processInfoBuilder.setRetryCount(1);
            processInfoBuilder.setJoinServerLog(false);
        }
        adLogBuilder.setProcessInfo(processInfoBuilder);
        adLogBuilder.setSourceType(adClientLog.getSourceType());
        adLogBuilder.setPosId(adClientLog.getPosId());
        adLogBuilder.setAccountId(adClientLog.getAccountId());
        adLogBuilder.setCreativeId(adClientLog.getCreativeId());
        adLogBuilder.setUnitId(adClientLog.getUnitId());
        switch (adClientLog.getEventType()) {
            case "SEND":
                adLogBuilder.setSend(1);
                break;
            case "IMPRESSION":
                adLogBuilder.setImpression(1);
                break;
            case "CLICK":
                adLogBuilder.setClick(1);
                break;
            case "DOWNLOAD":
                adLogBuilder.setDownload(1);
                break;
            case "INSTALLED":
                adLogBuilder.setInstalled(1);
                break;
            case "PAY":
                adLogBuilder.setPay(1);
                break;
            default:
                break;
        }
        return adLogBuilder.build();
    }

    public static void joinContext(AdLog.Builder adLogBuilder, AdServerLog context) {
        adLogBuilder.setGender(context.getGender());
        adLogBuilder.setAge(context.getAge());
        adLogBuilder.setCountry(context.getCountry());
        adLogBuilder.setProvince(context.getProvince());
        adLogBuilder.setCity(context.getCity());

        adLogBuilder.setBidPrice(context.getBidPrice());
    }

    static final String RETRY_TOPIC = Constants.CLIENT_LOG_RETRY;

    public static void sendRetry(Producer producer, byte[] value) {
        sendKafka(producer, RETRY_TOPIC, value);
    }

    public static void sendKafka(Producer producer, String topic, byte[] value) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, value);
        producer.send(record);
    }

    public static AdLogDTO buildAdLogDTO(AdLog adLog) {
        AdLogDTO adLogDTO = new AdLogDTO();
        adLogDTO.setRequestId(adLog.getRequestId());
        adLogDTO.setTimestamp(adLog.getTimestamp());
        adLogDTO.setDeviceId(adLog.getDeviceId());
        adLogDTO.setOs(adLog.getOs());
        adLogDTO.setNetwork(adLog.getNetwork());
        adLogDTO.setUserId(adLog.getUserId());
        adLogDTO.setGender(adLog.getGender());
        adLogDTO.setAge(adLog.getAge());
        adLogDTO.setCountry(adLog.getCountry());
        adLogDTO.setProvince(adLog.getProvince());
        adLogDTO.setCity(adLog.getCity());
        adLogDTO.setSourceType(adLog.getSourceType());
        adLogDTO.setBidType(adLog.getBidType());
        adLogDTO.setBidPrice(adLog.getBidPrice());
        adLogDTO.setPosId(adLog.getPosId());
        adLogDTO.setAccountId(adLog.getAccountId());
        adLogDTO.setCreativeId(adLog.getCreativeId());
        adLogDTO.setUnitId(adLog.getUnitId());
        adLogDTO.setEventType(adLog.getEventType());
        adLogDTO.setSend(adLog.getSend());
        adLogDTO.setImpression(adLog.getImpression());
        adLogDTO.setClick(adLog.getClick());
        adLogDTO.setDownload(adLog.getDownload());
        adLogDTO.setInstalled(adLog.getInstalled());
        adLogDTO.setPay(adLog.getPay());
        return adLogDTO;
    }
}
