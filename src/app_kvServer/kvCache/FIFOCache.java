package app_kvServer.kvCache;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class FIFOCache implements IKVCache{

    private final int capacity;
    private final Map<String, String> cache;
    private final Queue<String> orderQueue;

    public FIFOCache(int maxSize) {
        this.capacity = maxSize;
        this.cache = new HashMap<>();
        this.orderQueue = new LinkedList<>();
    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    @Override
    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    @Override
    public String getKV(String key) {
        return cache.get(key);
    }

    @Override
    public SimpleEntry<String, String> putKV(String key, String value) {

        if (capacity <= 0) return new SimpleEntry<>(key, value);

        SimpleEntry<String, String> evicted = null;

        if (cache.size() >= capacity) {
            String removeKey = orderQueue.poll();
            String removeValue = cache.remove(removeKey);
            evicted = new SimpleEntry<>(removeKey, removeValue);
        }

        orderQueue.add(key);
        cache.put(key, value);
        return evicted;
    }

    @Override
    public void updateKV(String key, String value) {
        if (cache.containsKey(key)) {
            cache.put(key, value);
        }
    }

    @Override
    public void deleteKV(String key) {
        if (cache.remove(key) != null) {
            orderQueue.remove(key);
        }
    }

    @Override
    public void clearCache() {
        cache.clear();
        orderQueue.clear();
    }

    public Map<String, String> getStoredData() {
        return new HashMap<>(this.cache);
    }

}
