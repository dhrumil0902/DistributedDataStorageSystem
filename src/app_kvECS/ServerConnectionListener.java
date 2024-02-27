package app_kvECS;

// Define the listener interface
public interface ServerConnectionListener {
    String onMessageReceived(String message, int port, String address);
    void onConnectionClosed();
}
