package shared.messages;

import java.io.Serializable;

import ecs.ECSNode;
import shared.BST;

public class KVMessageImpl implements KVMessage, Serializable {

    private String key;
    private String value;
    private StatusType status;
    private BST metadata;
    private String errorMessage;

    public KVMessageImpl() {};
    
    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessageImpl(StatusType status) {
        this.status = status;
    }

    public KVMessageImpl(String messageWithKV, StatusType status) {
        String[] splitMessage = messageWithKV.split(" ");

        if (splitMessage.length  < 3) {
            throw new IllegalArgumentException("Invalid message format");
        }
        
        this.status = status;
        this.key = splitMessage[1];
        this.value = messageWithKV.substring(splitMessage[0].length() + splitMessage[1].length() + 2);
    }

    /*
     * Factory for metadata given string format:
     * KEYRANGE_SUCCESS <range_from>,<range_to>,<ip:port>;...
     */
    public static KVMessageImpl fromKeyRange(String message, StatusType status) throws IllegalArgumentException {
        IllegalArgumentException e = new IllegalArgumentException("keyrange message should follow the format: KEYRANGE_SUCCESS <range_from>,<range_to>,<ip:port>;");
        KVMessageImpl kvMessage = new KVMessageImpl(status);
        kvMessage.status = status;
        kvMessage.metadata = new BST();
        String[] splitMessage = message.split(" ");
        if (splitMessage.length  != 2) throw e;
        String[] keyranges = splitMessage[1].split(";");
        for (String keyrange : keyranges) {
            String[] splitKeyrange = keyrange.split(",");
            if (splitKeyrange.length != 3) throw e;
            String[] ipPort = splitKeyrange[2].split(":");
            if (ipPort.length != 2) throw e;
            ECSNode node = new ECSNode(ipPort[0] + ":" + ipPort[1], ipPort[0], Integer.parseInt(ipPort[1]), new String[]{splitKeyrange[0], splitKeyrange[1]});
            kvMessage.metadata.put(splitKeyrange[1], node);
        }
        return kvMessage;
    }

    public String getKeyrangeString() {
        if (metadata == null) {
            throw new IllegalArgumentException("Cannot build keyrange string: metadata does not exist");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("KEYRANGE_SUCCESS ");
        for (String key : metadata.keys()) {
            ECSNode node = metadata.bst.get(key);
            sb.append(node.getNodeHashRange()[0])
                .append(",")
                .append(node.getNodeHashRange()[1])
                .append(",")
                .append(node.getNodeHost() + ":" + node.getNodePort())
                .append(";");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public KVMessageImpl(BST metadata, StatusType status) {
        this.metadata = metadata;
        this.status = status;
    }

    @Override
    public String getKey() {return key;}

    @Override
    public void setKey(String key) {this.key = key;}

    @Override
    public String getValue() {return value;}

    @Override
    public void setValue(String value) {this.value = value;}

    @Override
    public StatusType getStatus() {return status;}

    @Override
    public void setStatus(StatusType status) {this.status = status;}

    @Override
    public void setMetadata(BST metadata) {this.metadata = metadata;}

    @Override
    public BST getMetadata() {return metadata;}


    public static KVMessage fromString(String message) throws IllegalArgumentException {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("Message is null or empty");
        }
        String[] splitMessage = message.split(" ");

        try {
            StatusType status = KVMessage.StatusType.valueOf(splitMessage[0].toUpperCase());
            switch (status) {
                case GET:
                    return new KVMessageImpl(splitMessage[1], null, status);
                case GET_ERROR:
                    return new KVMessageImpl(splitMessage[1], null, status);
                case GET_SUCCESS:
                    return new KVMessageImpl(message, status);
                case PUT:
                    return new KVMessageImpl(message, status);
                case PUT_SUCCESS:
                    return new KVMessageImpl(message, status);
                case PUT_UPDATE:
                    return new KVMessageImpl(message, status);
                case PUT_ERROR:
                    return new KVMessageImpl(message, status);
                case DELETE_SUCCESS:
                    return new KVMessageImpl(splitMessage[1], null, status);
                case DELETE_ERROR:
                    return new KVMessageImpl(splitMessage[1], null, status);
                case SERVER_NOT_RESPONSIBLE:
                    return new KVMessageImpl(status);
                case SERVER_WRITE_LOCK:
                    return new KVMessageImpl(status);
                case SERVER_STOPPED:
                    return new KVMessageImpl(status);
                case KEYRANGE:
                    return new KVMessageImpl(status);
                case KEYRANGE_ERROR:
                    return new KVMessageImpl(status);
                case KEYRANGE_SUCCESS:
                    return KVMessageImpl.fromKeyRange(message, status);
                case DISCONNECT:
                    return new KVMessageImpl(status);
                default:
                    throw new IllegalArgumentException("Invalid status: " + splitMessage[0]);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unexpected error converting string to KVMessage: " + message, e);
        }
    }

    @Override
    public String toString() {
        switch (status) {
            case GET:
                return "get " + key;
            case GET_ERROR:
                return "GET_ERROR " + key;
            case GET_SUCCESS:
                return "GET_SUCCESS " + key + " " + value;
            case PUT:
                return "put " + key + " " + value;
            case PUT_SUCCESS:
                return "PUT_SUCCESS " + key + " " + value;
            case PUT_UPDATE:
                return "PUT_UPDATE " + key + " " + value;
            case PUT_ERROR:
                return "PUT_ERROR " + key + " " + value;
            case DELETE_SUCCESS:
                return "DELETE_SUCCESS " + key;
            case DELETE_ERROR:
                return "DELETE_ERROR " + key;
            case SERVER_NOT_RESPONSIBLE:
                return "SERVER_NOT_RESPONSIBLE";
            case SERVER_WRITE_LOCK:
                return "SERVER_WRITE_LOCK";
            case SERVER_STOPPED:
                return "SERVER_STOPPED";
            case KEYRANGE:
                return "keyrange";
            case KEYRANGE_ERROR:
                return "KEYRANGE_ERROR";
            case KEYRANGE_SUCCESS:
                return getKeyrangeString();
            
            default:
                return "FAILED Unexpected status type when serializing";
        }
    }
}
