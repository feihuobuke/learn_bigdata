package com.reiser.stream.entity.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class AdLogDTO {
    @JsonProperty("request_id")
    private String requestId;
    private long timestamp;
    @JsonProperty("device_id")
    private String deviceId;
    private String os;
    private String network;
    // dmp;
    @JsonProperty("user_id")
    private String userId;
    private String gender;
    private int age;
    private String country;
    private String province;
    private String city;
    // ad;
    @JsonProperty("source_type")
    private String sourceType;
    @JsonProperty("bid_type")
    private String bidType;
    @JsonProperty("bid_price")
    private long bidPrice;
    @JsonProperty("pos_id")
    private long posId;
    @JsonProperty("account_id")
    private long accountId;
    @JsonProperty("creative_id")
    private long creativeId;
    @JsonProperty("unit_id")
    private long unitId;
    // event;
    @JsonProperty("event_type")
    private String eventType;
    private long send;
    private long impression;
    private long click;
    private long download;
    private long installed;
    private long pay;
}
