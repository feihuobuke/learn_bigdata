package com.reiser.stream.mock;

import com.reiser.stream.entity.AdClientLog;
import com.reiser.stream.entity.AdServerLog;
import com.reiser.stream.untils.Constants;
import com.reiser.stream.untils.ETLUtils;
import com.reiser.stream.untils.KafkaProducerUtils;
import org.apache.kafka.clients.producer.Producer;

import java.util.Random;

public class MockData {

    Producer producer = KafkaProducerUtils.getProducer();
    int magicNum = 30;

    public void mockServerLog(String requestId, String deviceId, String os, String network, String userId, String sourceType,
                              String bidType, long posId, long accountId, long unitId, long creativeId, String eventType) {
        AdServerLog serverLog = AdServerLog.newBuilder()
                .setRequestId(requestId)
                .setTimestamp(System.currentTimeMillis())
                .setDeviceId(deviceId)
                .setOs(os)
                .setNetwork(network)
                .setUserId(userId)
                .setSourceType(sourceType)
                .setBidType(bidType)
                .setPosId(posId)
                .setAccountId(accountId)
                .setUnitId(unitId)
                .setCreativeId(creativeId)
                .setEventType(eventType)

                .setGender(MockDataUtils.gender())
                .setAge(MockDataUtils.age())
                .setCountry(MockDataUtils.country())
                .setProvince(MockDataUtils.province())
                .setBidPrice(MockDataUtils.bidPrice())

                .build();
        ETLUtils.sendKafka(producer, Constants.SERVER_LOG, serverLog.toByteArray());
    }

    public void mockClientLog(String requestId, String deviceId, String os, String network, String userId, String sourceType,
                              String bidType, long posId, long accountId, long unitId, long creativeId, String eventType ) {
        AdClientLog clientLog = AdClientLog.newBuilder()
                .setRequestId(requestId)
                .setTimestamp(System.currentTimeMillis())
                .setDeviceId(deviceId)
                .setOs(os)
                .setNetwork(network)
                .setUserId(userId)
                .setSourceType(sourceType)
                .setBidType(bidType)
                .setPosId(posId)
                .setAccountId(accountId)
                .setUnitId(unitId)
                .setCreativeId(creativeId)
                .setEventType(eventType)
                .build();
        ETLUtils.sendKafka(producer, Constants.CLIENT_LOG, clientLog.toByteArray());
    }


    public void mock() {
        String requestId = MockDataUtils.requestId();
        String deviceId = MockDataUtils.deviceId();
        String os = MockDataUtils.os();
        String network = MockDataUtils.network();
        String userId = MockDataUtils.userId();
        String sourceType = MockDataUtils.sourceType();
        String bidType = MockDataUtils.bidType();
        long posId = MockDataUtils.posId();
        long accountId = MockDataUtils.accountId();
        long unitId = MockDataUtils.unitId(accountId);
        long creativeId = MockDataUtils.creativeId(unitId);

        mockServerLog(requestId, deviceId, os, network, userId, sourceType, bidType, posId, accountId, unitId, creativeId, "SEND");
        if (isImpression(creativeId)) {
            mockClientLog(requestId, deviceId, os, network, userId, sourceType, bidType, posId, accountId, unitId, creativeId, "IMPRESSION");
            if (isClick(creativeId)) {
                mockClientLog(requestId, deviceId, os, network, userId, sourceType, bidType, posId, accountId, unitId, creativeId, "CLICK");
                if (isDownload(creativeId)) {
                    mockClientLog(requestId, deviceId, os, network, userId, sourceType, bidType, posId, accountId, unitId, creativeId, "DOWNLOAD");
                    if (isinstalled(creativeId)) {
                        mockClientLog(requestId, deviceId, os, network, userId, sourceType, bidType, posId, accountId, unitId, creativeId, "INSTALLED");
                        if (isPayed(creativeId)) {
                            System.out.println("mock pay");
                            mockClientLog(requestId, deviceId, os, network, userId, sourceType, bidType, posId, accountId, unitId, creativeId, "PAY");
                        }
                    }
                }
            }
        }

    }

    public boolean isImpression(long creativeId) {
        int base = new Random(creativeId).nextInt(10);
        return new Random().nextInt(37 + base) < base + magicNum;
    }

    public boolean isClick(long creativeId) {
        int base = new Random(creativeId).nextInt(10);
        return new Random().nextInt(47 + base) < base + magicNum;
    }

    public boolean isDownload(long creativeId) {
        int base = new Random(creativeId).nextInt(10);
        return new Random().nextInt(57 + base) < base + magicNum;
    }

    public boolean isinstalled(long creativeId) {
        int base = new Random(creativeId).nextInt(10);
        return new Random().nextInt(67 + base) < base + magicNum;
    }


    public boolean isPayed(long creativeId) {
        int base = new Random(creativeId).nextInt(10);
        return new Random().nextInt(83 + base) < base + magicNum;
    }


    public static void main(String[] args) throws InterruptedException {
        MockData mockData = new MockData();
        while(true) {
            mockData.mock();
            Thread.sleep(100);
        }
    }

}
