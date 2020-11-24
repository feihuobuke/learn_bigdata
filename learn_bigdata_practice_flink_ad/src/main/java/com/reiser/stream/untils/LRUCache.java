package com.reiser.stream.untils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author: reiserx
 * Date:2020/11/5
 * Des: 替换算法
 * LinkedHashMap 本身就实现了 LRUCache
 */
class LRUCache extends LinkedHashMap<byte[], byte[]> {
    private int capacity;

    public LRUCache(int capacity) {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
    }

    public byte[] get(byte[] key) {
        return super.getOrDefault(key, null);
    }

    public byte[] put(byte[] key, byte[] value) {
        super.put(key, value);
        return key;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<byte[], byte[]> eldest) {
        return size() > capacity;
    }
}
