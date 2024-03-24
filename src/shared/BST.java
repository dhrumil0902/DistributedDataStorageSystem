package shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecs.ECSNode;
import ecs.IECSNode;

import java.io.Serializable;
import java.util.*;
import java.io.*;


@JsonIgnoreProperties(ignoreUnknown = true)
public class BST implements Serializable {
    public TreeMap<String, ECSNode> bst;

    public BST() {
        bst = new TreeMap<>();
    }
    // Serialize the BST to a byte array
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(this);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

    // Deserialize a byte array to reconstruct the BST
    public static BST deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        BST bst = (BST) objectInputStream.readObject();
        objectInputStream.close();
        return bst;
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

    public Collection<ECSNode> values() {
        return bst.values();
    }

    public IECSNode higherEntry(String key) {
        if (bst.isEmpty()) {
            return null;
        }

        Map.Entry<String, ECSNode> entry = bst.higherEntry(key);
        if (entry == null) {
            return bst.lastEntry().getValue();
        }
        return entry.getValue();
    }

    public String print() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ECSNode> entry : bst.entrySet()) {
            sb.append("Key: ").append(entry.getKey())
                    .append(", Range: ").append(Arrays.toString(entry.getValue().getNodeHashRange()))
                    .append(", NodeName: ").append(entry.getValue().getNodeName())
                    .append("\n");
        }
        return sb.toString();
    }
}