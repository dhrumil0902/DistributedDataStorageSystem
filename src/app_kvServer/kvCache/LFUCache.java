package app_kvServer.kvCache;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

public class LFUCache implements IKVCache{
    private final int capacity;
    private int minFreq;
    private final Map<String, String> key2Val;
    private final Map<String, Integer> key2Freq;
    private final Map<Integer, LinkedHashSet<String>> freq2Keys;

    public  LFUCache(int maxSize) {
        capacity = maxSize;
        minFreq = 0;
        key2Val = new HashMap<>();
        key2Freq = new HashMap<>();
        freq2Keys = new HashMap<>();
    }

    @Override
    public int getCacheSize() {
        return key2Val.size();
    }

    @Override
    public boolean inCache(String key) {
        return key2Val.containsKey(key);
    }

    @Override
    public String getKV(String key) {
        if (!key2Val.containsKey(key)) {
            return null;
        }
        updateFreq(key);
        return key2Val.get(key);
    }

    @Override
    public SimpleEntry<String, String> putKV(String key, String value) {
        if (capacity <= 0) return null;

        SimpleEntry<String, String> evictedEntry = null;
        if (key2Val.size() >= capacity) {
            evictedEntry = evict();
        }

        key2Val.put(key, value);
        key2Freq.put(key, 1);
        freq2Keys.putIfAbsent(1, new LinkedHashSet<String>());
        freq2Keys.get(1).add(key);
        this.minFreq = 1;
        return evictedEntry;
    }

    @Override
    public void updateKV(String key, String value) {
        if (key2Val.containsKey(key)) {
            updateFreq(key);
            key2Val.put(key, value);
        }
    }

    @Override
    public void deleteKV(String key) {
        if (!key2Val.containsKey(key)) {
            return;
        }

        key2Val.remove(key);
        int freq = key2Freq.get(key);
        key2Freq.remove(key);
        freq2Keys.get(freq).remove(key);

        if (freq2Keys.get(freq).isEmpty()) {
            freq2Keys.remove(freq);
            if (freq == minFreq) {
                adjustMinFreq();
            }
        }
    }

    @Override
    public void clearCache() {
        key2Val.clear();
        key2Freq.clear();
        freq2Keys.clear();
        minFreq = 0;
    }

    @Override
    public Map<String, String> getStoredData() {
        return new HashMap<>(this.key2Val);
    }

    private void updateFreq(String key) {
        int freq = key2Freq.get(key);
        key2Freq.put(key, freq + 1);
        freq2Keys.putIfAbsent(freq + 1, new LinkedHashSet<String>());
        freq2Keys.get(freq + 1).add(key);
        freq2Keys.get(freq).remove(key);

        if (freq2Keys.get(freq).isEmpty()) {
            freq2Keys.remove(freq);
            if (freq == minFreq) {
                minFreq += 1;
            }
        }
    }

    private SimpleEntry<String, String> evict() {
        adjustMinFreq();
        String evictKey = freq2Keys.get(this.minFreq).iterator().next();
        String evictValue = key2Val.get(evictKey);
        key2Val.remove(evictKey);
        key2Freq.remove(evictKey);

        freq2Keys.get(this.minFreq).remove(evictKey);
        if (freq2Keys.get(this.minFreq).isEmpty()) {
            freq2Keys.remove(this.minFreq);
        }
        return new SimpleEntry<>(evictKey, evictValue);
    }

    private void adjustMinFreq() {
        if (freq2Keys.containsKey(minFreq) && !freq2Keys.get(minFreq).isEmpty()) {
            return;
        }
        for (int freq = minFreq + 1; freq <= Collections.max(freq2Keys.keySet()); freq++) {
            if (freq2Keys.containsKey(freq) && !freq2Keys.get(freq).isEmpty()) {
                minFreq = freq;
                return;
            }
        }
    }
}
