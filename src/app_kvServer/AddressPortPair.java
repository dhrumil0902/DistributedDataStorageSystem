package app_kvServer;

public class AddressPortPair {
    private String address;
    private int port;

    public AddressPortPair(String address, int port) {
        this.address = address;
        this.port = port;
    }

    // Getters
    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return address + ":" + port;
    }
}
