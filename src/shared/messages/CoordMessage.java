package shared.messages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CoordMessage implements Serializable {
    public enum ActionType {
        PUT,
        UPDATE,
        DELETE,
        SYNC
    }

    private ActionType action;
    private String key;
    private String value;
    private List<String> data;
    public boolean isSuccess;

    public CoordMessage() {
        this.action = null;
        this.isSuccess = false;
        this.data = new ArrayList<>();
        this.key = null;
        this.value = null;
    }

    public ActionType getAction() {
        return this.action;
    }

    public void setAction(ActionType action) {this.action = action;}

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {this.key = key;}

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {this.value = value;}
}
