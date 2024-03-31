package shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecs.ECSNode;
import ecs.IECSNode;

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
        String pred = bst.lowerKey(key);
        if (pred == null) {
            return max();
        }
        return pred;
    }

    public String successor(String key) {
        String succ = bst.higherKey(key);
        if (succ == null) {
            return min();
        }
        return succ;
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

    public IECSNode getNodeFromKey(String key) {
        if (bst.isEmpty()) {
            return null;
        }

        Map.Entry<String, ECSNode> entry = bst.higherEntry(key);
        if (entry == null) {
            return bst.firstEntry().getValue();
        }
        return entry.getValue();
    }

    // Create a new BST with the same nodes, where each node is responsible for replicating the ranges of the previous 2 nodes
    public BST createReplicatedRange() {
        BST replicatedRange = new BST();
        String[] keys = new String[bst.size()];
        int i = 0;
        for (String key : bst.keySet()) {
            keys[i++] = key;
        }
        for (i = 0; i < keys.length; i++) {
            String key = keys[i];
            IECSNode node = get(key);
            String pred = predecessor(key);
            if (predecessor(pred) != key) pred = predecessor(pred);
            IECSNode predNode = get(pred);

            String[] hashRange = {predNode.getNodeHashRange()[0], node.getNodeHashRange()[1]};

            ECSNode newNode = new ECSNode(node.getNodeName(), node.getNodeHost(), node.getNodePort(), hashRange);
            replicatedRange.put(key, newNode);
        }
        return replicatedRange;
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