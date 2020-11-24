package com.reiser.stream.untils;

/**
 * @author: reiserx
 * Date:2020/11/17
 * Des:
 */
public class RedisUtils {

//    static JedisPool jedisPool;
//
//    static {
//        final JedisPoolConfig poolConfig = new JedisPoolConfig();
//        poolConfig.setMaxTotal(128);
//        poolConfig.setMaxIdle(128);
//        poolConfig.setMinIdle(16);
//        poolConfig.setTestOnBorrow(true);
//        poolConfig.setTestOnReturn(true);
//        poolConfig.setTestWhileIdle(true);
//        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
//        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
//        poolConfig.setNumTestsPerEvictionRun(3);
//        poolConfig.setBlockWhenExhausted(true);
//        jedisPool = new JedisPool(poolConfig, "192.168.91.1", 6379);
//    }
//
//    public static Jedis initRedis() {
//        return jedisPool.getResource();
//    }
}
