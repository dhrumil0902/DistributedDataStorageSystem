package shared.messages;

import java.io.Serializable;

public class KVMessageImpl implements KVMessage, Serializable {

    private String key;
    private String value;
    private StatusType status;

    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessageImpl() {
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    public static KVMessage fromString(String message) throws IllegalArgumentException {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("Message is null or empty");
        }
        String[] splitMessage = message.split(" ");
        StatusType status;
        String key;
        String value;

        if (splitMessage.length < 2) {
            throw new IllegalArgumentException("Message requires at least a status and a key, got: " + message);
        }

        status = KVMessage.StatusType.valueOf(splitMessage[0].toUpperCase());
        key = splitMessage[1];
        if (splitMessage.length == 2) {
            value = "";
        } else {
            value = message.substring(splitMessage[0].length() + splitMessage[1].length() + 2);
        }

        return new KVMessageImpl(key, value, status);
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
            default:
                return "FAILED Unexpected status type when serializing";
        }
    }
}
