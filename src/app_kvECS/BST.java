package app_kvECS;

import ecs.ECSNode;
import ecs.IECSNode;

import java.nio.file.Path;
import java.util.*;

public class BST {
    public TreeMap<String, IECSNode> bst;

    public BST() {
        bst = new TreeMap<>();
    }

    public void put(String key, ECSNode node) {
        bst.put(key, node);
    }

    public IECSNode get(String key) {
        return bst.get(key);
    }

    public boolean contains(String key) {
        return bst.containsKey(key);
    }

    public String predecessor(String key) {
        return bst.lowerKey(key);
    }

    public String successor(String key) {
        return bst.higherKey(key);
    }

    public void delete(String key) {
        bst.remove(key);
    }

    public int size() {
        return bst.size();
    }

    public boolean isEmpty() {
        return bst.isEmpty();
    }

    public String min() {
        return bst.firstKey();
    }

    public String max() {
        return bst.lastKey();
    }

    public Iterable<String> keys() {
        return bst.keySet();
    }

    public Iterable<String> keys(String low, String high) {
        return bst.subMap(low, true, high, true).keySet();
    }

    public Iterable<String> sortedKeys() {
        return bst.navigableKeySet();
    }

    public Collection<IECSNode> values() {
        return bst.values();
    }
}