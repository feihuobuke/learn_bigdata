package com.reiser.xeye.etl;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author: reiserx
 * Date:2020/11/8
 * Des:
 */
public class Test {
    public static void main(String[] args) {
        Jedis redis = new Jedis("localhost", 6379);
        List<String> strings = redis.pubsubChannels("__keyspace@0__:*");
    }
}
