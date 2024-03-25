package shared.messages;

import java.io.Serializable;
import java.util.List;

public class CoordMessage implements Serializable {
    public enum ActionType {
        PUT,
        UPDATE,
        DELETE,
        SYNC
    }
    private ECSMessage.ActionType action;
    private String key;
    private String value;
    private List<String> data;
    public boolean isSuccess;
}
