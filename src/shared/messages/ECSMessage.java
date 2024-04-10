package shared.messages;
import java.io.*;
import java.util.*;

import shared.BST;

public class ECSMessage implements Serializable {

    public enum ActionType {
        NEW_NODE,
        DELETE, //stop the server (keeps running but returns SERVER_STOPPED to client) returns (success = true)
        SET_WRITE_LOCK, // sets write lock, returns success = true
        UNSET_WRITE_LOCK, //unset write lock, return sucess = true
        TRANSFER, // returns all the data, sucess= true , data has the list of KV pairs
        APPEND, // appends the given KV pairs stored in "data" field, return successs = true
        REMOVE, // remove all keys in the range field, and return those KV pairs, stored in data. success = true
        UPDATE_METADATA, // "nodes" field is filled with metadata, simply update kvserver's "nodes" field and return success = true
        HEARTBEAT,
        None, INTERNAL_TRANSFER

    }

    private ActionType action;
    private String[] serverInfo;
    public boolean success = false;
    public List<String> data;
    public String[] range;
    public BST nodes;
    public String internalTransferHash;
    private String errorMessage = null;

    public ECSMessage() {
        this.action = null;
        this.success = false;
        this.data = new ArrayList<>();
        this.range = null;
        this.serverInfo = null;
        this.nodes = null;
    }

    public ECSMessage(ActionType action, boolean success, List<String> data, String[] range, BST nodes) {
        this.setAction(action);
        this.success = success;
        this.data = data;
        this.range = range;
        this.nodes = nodes;
    }
    public ECSMessage(ActionType action, boolean success, List<String> data, String[] range, BST nodes, String internalTransferHash) {
        this.setAction(action);
        this.success = success;
        this.data = data;
        this.range = range;
        this.nodes = nodes;
        this.internalTransferHash = internalTransferHash;
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

    public boolean getSuccess() {return this.success;}

    public void setSuccess(Boolean status) {
        this.success = status;
    }

    public String[] getServerInfo() {return this.serverInfo;}

    public void setServerInfo(String address, int port) {
        this.serverInfo = new String[]{address, String.valueOf(port)};
    }

    public BST getNodes() {return this.nodes;}

    public void setNodes(BST nodes) {this.nodes = nodes;}

    public String getErrorMessage() {return this.errorMessage;}

    public void setErrorMessage(String errorMessage) {this.errorMessage = errorMessage;}

    public String[] getRange() {return this.range;}

    public void setRange(String[] range) {this.range = range;}
}
