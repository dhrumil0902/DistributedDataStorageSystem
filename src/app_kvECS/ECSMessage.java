package app_kvECS;
import java.io.*;
import java.util.*;

public class ECSMessage implements Serializable {

    public enum ActionType {
        DELETE, //stop the server (keeps running but returns SERVER_STOPPED to client) returns (success = true)
        SET_WRITE_LOCK, // sets write lock, returns success = true
        UNSET_WRITE_LOCK, //unset write lock, return sucess = true
        GET_ALL, // returns all the data, sucess= true , data has the list of KV pairs
        APPEND, // appends the given KV pairs stored in "data" field, return successs = true
        REMOVE, // remove all keys in the range field, and return those KV pairs, stored in data. success = true
        UPDATE_METADATA // "nodes" field is filled with metadata, simply update kvserver's "nodes" field and return success = true

    }

    private ActionType action;
    public boolean success = false;
    public List<String> data;

    public String[] range;

    public BST nodes;

    public ECSMessage() {
        this.action = null;
        this.success = false;
        this.data = new ArrayList<>();
        this.range = new String[0];
        this.nodes = null;
    }

    public ECSMessage(ActionType action, boolean success, List<String> data, String[] range, BST nodes) {
        this.setAction(action);
        this.success = success;
        this.data = data;
        this.range = range;
        this.nodes = nodes;
    }

    public ActionType getAction() {
        return action;
    }

    public void setAction(ActionType action) {
        this.action = action;
    }

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }

    public void setSuccess(Boolean status) {
        this.success = status;
    }
}

class Main {
    public static void main(String[] args) {
        // Example data - JUST FOR TESTING PURPOSES
        ActionType action = ActionType.SET_WRITE_LOCK;
        boolean success = true;
        List<String> dataList = Arrays.asList("data1", "data2", "data3");

        ECSMessage ecsMessage = new ECSMessage(action, success, dataList, null, null);

        String serializedString = serializeToString(ecsMessage);
        System.out.println("Serialized String: " + serializedString);

        // Deserialize string
        ECSMessage deserializedMessage = deserializeFromString(serializedString);
        System.out.println("Action: " + deserializedMessage.getAction() + ", Success: " + deserializedMessage.success);

        for (String item : deserializedMessage.data) {
            System.out.println("Data: " + item);
        }
    }

     static String serializeToString(Object obj) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(obj);
            objectOutputStream.close();
            return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

     static ECSMessage deserializeFromString(String serializedString) {
        try {
            byte[] data = Base64.getDecoder().decode(serializedString);
            ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
            return (ECSMessage) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}