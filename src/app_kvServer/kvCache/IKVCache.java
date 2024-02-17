package app_kvServer.kvCache;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

public interface IKVCache {

    public int getCacheSize();

    public boolean inCache(String key);

    public String getKV(String key);

    public SimpleEntry<String, String> putKV(String key, String value);

    public void updateKV(String key, String value);

    public void deleteKV(String key);

    public void clearCache();

    public Map<String, String> getStoredData();
}
