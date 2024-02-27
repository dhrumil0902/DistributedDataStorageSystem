package app_kvServer;

// Define the listener interface
public interface ServerConnectionListener {
    String onMessageReceived(String message);
}
