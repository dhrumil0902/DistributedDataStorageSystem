package app_kvServer.kvCache;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache implements IKVCache{
    private final int capacity;
    private final Map<String, String> cache;
    private SimpleEntry<String, String> lastEvicted = null;

    public LRUCache(int maxSize) {
        capacity = maxSize;
        cache = new LinkedHashMap<String, String>(capacity, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                boolean evict = size() > LRUCache.this.capacity;
                if (evict) {
                    lastEvicted = new SimpleEntry<>(eldest.getKey(), eldest.getValue());
                }
                return evict;
            }
        };

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
        if (capacity <= 0) return null;
        lastEvicted = null;
        cache.put(key, value);
        return lastEvicted;
    }

    @Override
    public void updateKV(String key, String value) {
        putKV(key, value);
    }

    @Override
    public void deleteKV(String key) {
        cache.remove(key);
    }

    @Override
    public void clearCache() {
        cache.clear();
    }

    @Override
    public Map<String, String> getStoredData() {
        return new HashMap<>(this.cache);
    }
}
