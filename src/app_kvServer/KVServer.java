package app_kvServer;

import java.io.File;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.io.IOException;

import app_kvServer.kvCache.*;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;


import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public class KVServer implements IKVServer, ServerConnectionListener, Runnable {
    /**
     * Start KV Server at given port
     *
     * @param port given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     * to keep in-memory
     * @param strategy specifies the cache replacement strategy in case the cache
     * is full and there is a GET- or PUT-request on a key that is
     * currently not contained in the cache. Options are "FIFO", "LRU",
     * and "LFU".
     */
    private static final Logger logger = Logger.getRootLogger();
    int cacheSize;
    CacheStrategy strategy;
    private int clientPort;
    private int serverPort;
    private ServerSocket clientServerSocket;
    private ServerSocket serverServerSocket;
    private boolean running;
    private String address;
    private final String storagePath;
    private final IKVCache cache;
    private final KVStorage storage;
    private final Object lock = new Object();
    private ExecutorService threadPool;

    private List<ClientConnection> clientConnections = new ArrayList<ClientConnection>();

//    public KVServer(int port, int cacheSize, String strategy) {
//        this(port, cacheSize, strategy, "localhost", System.getProperty("user.dir"));
//    }

    public KVServer(int clientPort, int serverPort, int cacheSize, String strategy, String address, String storageDir) {
        String fileName = address + "_" + clientPort + ".txt";
        this.storagePath = storageDir + File.separator + fileName;
        this.address = address;
        this.clientPort = clientPort;
        this.serverPort = serverPort
        this.cacheSize = cacheSize;
        try {
            this.strategy = CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid cache strategy value: " + strategy + ". Setting strategy to None.");
            this.strategy = CacheStrategy.None;
        }
        switch (this.strategy) {
            case LRU:
                this.cache = new LRUCache(cacheSize);
                break;
            case LFU:
                this.cache = new LFUCache(cacheSize);
                break;
            case FIFO:
                this.cache = new FIFOCache(cacheSize);
                break;
            default:
                this.cache = new FIFOCache(0);
                break;
        }
        this.storage = new KVStorage(this.storagePath);
        startServer();
    }

    @Override
    public int getPort() {
        return clientPort;
    }

    @Override
    public String getHostname() {
        return address;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return strategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        try {
            return storage.inStorage(key);
        } catch (RuntimeException e) {
            logger.error("Error: Checking if key is in storage: " + key, e);
            return false;
        }
    }

    @Override
    public boolean inCache(String key) {
        if (cache != null) {
            return cache.inCache(key);
        }
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        logger.info("SERVER: Retrieve value for key: " + key);
        String value;
        if (cache != null && cache.inCache(key)) {
            value = cache.getKV(key);
        } else {
            try {
                value = storage.getKV(key);
                logger.info(String.format("Key: %s; Value: %s", key, value));
            } catch (RuntimeException e) {
                throw new Exception(String.format("Error retrieving Key: %s from storage。 %s", key, e.getMessage()));
            }
            if (cacheSize != 0 || value != null) {
                SimpleEntry<String, String> evicted = cache.putKV(key, value);
                if (evicted != null) {
                    try {
                        writeEvictedToStorage(evicted.getKey(), evicted.getValue());
                    } catch (RuntimeException e) {
                        throw new Exception(e.getMessage());
                    }
                }
                try {
                    storage.deleteKV(key);
                } catch (RuntimeException e) {
                    throw new Exception(String.format("Error deleting old Key: %s from storage. %s", key, e.getMessage()));
                }
            }

        }
        return value;
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        logger.info(String.format("PutKV: %s %s", key, value));
        // Put kv to storage
        if (cache == null) {
            try {
                storage.putKV(key, value);
                return;
            } catch (RuntimeException e) {
                throw new Exception(e.getMessage());
            }
        }
        // Put kv to cache
        SimpleEntry<String, String> evicted = cache.putKV(key, value);
        if (evicted != null) {
            try {
                writeEvictedToStorage(evicted.getKey(), evicted.getValue());
            } catch (RuntimeException e) {
                throw new Exception(e.getMessage());
            }
        }
    }

    public boolean deleteKV(String key) throws Exception{
        if (inCache(key)) {
            cache.deleteKV(key);
        } else if (inStorage(key)) {
            try {
                storage.deleteKV(key);
            } catch (RuntimeException e) {
                throw new Exception(e);
            }
        } else {
            return false;
        }
        return true;
    }

    public void updateStorage(String key, String value) throws Exception{
        // Update kv to storage
        if (cache == null) {
            try {
                storage.updateKV(key, value);
                return;
            } catch (RuntimeException e) {
                throw new Exception(e);
            }
        }
        // Update kv to cache
        SimpleEntry<String, String> evicted = cache.putKV(key, value);
        if (evicted != null) {
            try {
                writeEvictedToStorage(evicted.getKey(), evicted.getValue());
            } catch (RuntimeException e) {
                throw new Exception(e.getMessage());
            }
        }
        // Delete old kv from storage
        try {
            storage.deleteKV(key);
        } catch (RuntimeException e) {
            throw new Exception(String.format("Error deleting old Key: %s from storage. %s", key, e.getMessage()));
        }
    }

    public String getStoragePath() {
        return this.storagePath;
    }

    @Override
    public void clearCache() {
        if (cache != null) {
            cache.clearCache();
        }
    }

    @Override
    public void clearStorage() {
        try {
            storage.clearStorage();
        } catch (RuntimeException e) {
            logger.error(e);
        }
    }

    private void writeEvictedToStorage(String key, String value) throws RuntimeException{
        try {
            storage.putKV(key, value);
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format("Error writing evicted Key %s to disk. %s", key, e.getMessage()));
        }
    }

    public void syncCacheToStorage() {
        if (cacheSize != 0) {
            for (Map.Entry<String, String> entry : cache.getStoredData().entrySet()) {
                try {
                    storage.putKV(entry.getKey(), entry.getValue());
                } catch (RuntimeException e) {
                    logger.error("Failed to sync cache.", e);
                }
            }
        }
    }

    public void startServer() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        running = initializeServer();
        threadPool = Executors.newCachedThreadPool();

        if (clientServerSocket != null) {
            threadPool.execute(new ClientListener(clientServerSocket, this));
        }

        if (serverServerSocket != null) {
            threadPool.execute(new ServerListener(serverServerSocket, this));
        }
    }

    private class ClientListener implements Runnable {
        private final ServerSocket serverSocket;
        private final ServerConnectionListener connectionListener;

        public ClientListener(ServerSocket serverSocket, ServerConnectionListener connectionListener) {
            this.serverSocket = serverSocket;
            this.connectionListener = connectionListener;
        }

        public void run() {
            while (isRunning()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(clientSocket, connectionListener);
                    threadPool.execute(connection);

                    logger.info("Connected to client at " + clientSocket.getInetAddress().getHostName() +
                            " on port " + clientSocket.getPort());
                } catch (IOException e) {
                    logger.error("Error accepting client connection", e);
                }
            }
        }
    }

    private class ServerListener implements Runnable {
        private final ServerSocket serverSocket;
        private final KVServer server;

        public ServerListener(ServerSocket serverSocket, KVServer server) {
            this.serverSocket = serverSocket;
            this.server = server;
        }

        public void run() {
            while (isRunning()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ServerConnection connection = new ServerConnection(clientSocket, server);
                    threadPool.execute(connection);

                    logger.info("Connected to server at " + clientSocket.getInetAddress().getHostName() +
                            " on port " + clientSocket.getPort());
                } catch (IOException e) {
                    logger.error("Error accepting server connection", e);
                }
            }
        }
    }

    @Override
    public void kill() {
        logger.info("Killing server.");
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        logger.info("Closing server.");
        running = false;
        syncCacheToStorage();
        try {
            serverSocket.close();
            for (ClientConnection connection : clientConnections) {
                connection.close();
            }
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
        threadPool.shutdownNow();
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            InetSocketAddress clientSocketAddress = new InetSocketAddress(address, clientPort);
            clientServerSocket = new ServerSocket();
            clientServerSocket.bind(clientSocketAddress);
            logger.info("Client Socket listening on " + address + ":" + clientPort);
        } catch (BindException e) {
            logger.error("Error! Client port " + clientPort + " is already bound!", e);
            return false;
        } catch (IOException e) {
            logger.error("Error! Cannot open client server socket due to an I/O error.", e);
            return false;
        }

        try {
            InetSocketAddress serverSocketAddress = new InetSocketAddress(address, serverPort);
            serverServerSocket = new ServerSocket();
            serverServerSocket.bind(serverSocketAddress);
            logger.info("Server Socket listening on " + address + ":" + serverPort);
            return true;
        } catch (BindException e) {
            logger.error("Error! Server port " + serverPort + " is already bound!", e);
            return false;
        } catch (IOException e) {
            logger.error("Error! Cannot open server server socket due to an I/O error.", e);
            return false;
        }
    }

    private boolean isRunning() {
        return this.running;
    }

    @Override
    public String onMessageReceived(String message) {
        if (message == null || message.isEmpty()) {
            logger.error("SERVER: Received empty or null message from client.");
            return "FAILED Received empty or null message from client";
        }

        logger.info("SERVER: Received message from client: " + message);
        logger.info("SERVER: Parsing the message on the server side ...");

        String[] splitMessage = message.split(" ");

        if (splitMessage.length == 0) {
            logger.error("Invalid empty message received.");
            return "FAILED Invalid empty message received.";
        }

        String command = splitMessage[0];

        switch (command) {
            case "get":
                return handleGetMessage(splitMessage);

            case "put":
                return handlePutMessage(splitMessage);

            default:
                logger.error("Unknown command: " + command);
                return "FAILED unknown command " + command;
        }
    }

    private String handleGetMessage(String[] messageParts) {
        if (messageParts.length == 2) {
            synchronized (lock) {
                String key = messageParts[1];
                try {
                    logger.info("SERVER: Trying to GET the value associated with Key '" + key);
                    String value = getKV(key);
                    if (value == null || value.isEmpty()) {
                        return "GET_ERROR " + key;
                    }
                    return String.format("GET_SUCCESS %s %s", key, value);
                } catch (Exception e) {
                    logger.error("Error retrieving value for key '" + key + "': " + e.getMessage());
                    return "GET_ERROR " + key;
                }
            }
        }
        return "GET_ERROR " + messageParts[1];
    }

    private String handlePutMessage(String[] messageParts) {
        if (messageParts.length >= 3) {
            String key = messageParts[1];
            String value = String.join(" ", Arrays.copyOfRange(messageParts, 2, messageParts.length));
            String returnString = " ";
            synchronized (lock) {
                logger.debug("Got the Lock");
                // Delete
                if (value.equals("null")) {
                    try {
                        logger.info("SERVER: Key '" + key + "' deleted '");
                        if (deleteKV(key)) {
                            return "DELETE_SUCCESS " + key;
                        }
                        logger.info(String.format("SERVER: Delete key %s not exists.", key));
                        return "DELETE_ERROR " + key;
                    } catch (Exception e) {
                        logger.error("Error deleting key '" + key + " " + e.getMessage());
                        return "DELETE_ERROR " + key;
                    }
                }
                // Update
                if (inCache(key)) {
                    logger.info("SERVER: Update cache with Key '" + key + "', Value '" + value + "'");
                    cache.updateKV(key, value);
                    return "PUT_UPDATE " + key + " " + value;
                }
                if (inStorage(key)) {
                    logger.info("SERVER: Update storage with Key '" + key + "', Value '" + value + "'");
                    try {
                        updateStorage(key, value);
                        return "PUT_UPDATE " + key + " " + value;
                    } catch (Exception e) {
                        logger.error("Error updating key-value pair for key '" + key + "': " + e.getMessage());
                        return "PUT_ERROR " + key + " " + value;
                    }
                }

                //Put
                returnString = "PUT_SUCCESS";
                logger.info("SERVER: PUT  Key '" + key + "', Value '" + value + "'");
                try {
                    putKV(key, value);
                    return String.format("%s %s %s", returnString, key, value);
                } catch (Exception e) {
                    logger.error("Error putting key-value pair for key '" + key + "': " + e.toString());
                    return "PUT_ERROR " + key + " " + value;
                }
            }
        } else {
            logger.error("Invalid PUT message format: " + String.join(" ", messageParts));
            return "FAILED put request is not formatted correctly: " + String.join(" ", messageParts);
        }
    }

    @Override
    public void onConnectionClosed() {

        logger.info("Connection closed by client.");
    }

    public void setAddress(String address) {
        this.address = address;
    }

    private static String generateHelpString() {
        return "Usage: java KVServer [-p <port>] [-a <address>] [-d <directory>] [-l <logFile>] [-ll <logLevel>] [-c <cacheSize>] [-cs <cacheStrategy>]\n"
                + "Options:\n"
                + "  -p <port>          Port number for the KVServer (default: 5000)\n"
                + "  -a <address>       Address for the KVServer (default: localhost)\n"
                + "  -d <directory>     Directory for storage (default: current directory)\n"
                + "  -l <logFile>       File path for the log file (default: ./server.log)\n"
                + "  -ll <logLevel>     Log level for the server (default: ALL)\n"
                + "  -c <cacheSize>     Size of the cache (default: 10)\n"
                + "  -cs <cacheStrategy> Cache replacement strategy (default: None)\n\n"
                + "Example:\n"
                + "  java KVServer -p 8080 -a 127.0.0.1 -d /path/to/data -l /path/to/server.log -ll INFO -c 50 -cs LRU";
    }

    public static void main(String[] args) {

        String helpString = generateHelpString();

        // Default Values
        int port = 5000;
        String address = "localhost";
        String directory = System.getProperty("user.dir");
        String logFile = System.getProperty("user.dir") + "/server.log";
        Level logLevel = Level.ALL;
        CacheStrategy strategy = CacheStrategy.None;
        int cacheSize = 10;

        if (args.length > 0 && args[0].equals("-h")) {
            System.out.println(helpString);
            System.exit(1);
        }

        if (args.length % 2 != 0) {
            System.out.println("Invalid number of arguments");
            System.out.println(helpString);
            System.exit(1);
        }

        try {
            for (int i = 0; i < args.length; i += 2) {
                switch (args[i]) {
                    case "-p":
                        port = Integer.parseInt(args[i + 1]);
                        break;
                    case "-a":
                        address = args[i + 1];
                        break;
                    case "-d":
                        directory = args[i + 1];
                        break;
                    case "-l":
                        logFile = args[i + 1];
                        break;
                    case "-ll":
                        if (LogSetup.isValidLevel(args[i + 1]))
                            logLevel = Level.toLevel(args[i + 1]);
                        else {
                            System.out.println("Invalid log level: " + args[i + 1]);
                            System.out.println(helpString);
                            System.exit(1);
                        }
                        break;
                    case "-c":
                        cacheSize = Integer.parseInt(args[i + 1]);
                        break;
                    case "-cs":
                        strategy = CacheStrategy.valueOf(args[i + 1]);
                        break;
                    default:
                        System.out.println("Invalid argument: " + args[i]);
                        System.out.println(helpString);
                        System.exit(1);
                }
            }
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument -p! Not a number!");
            System.out.println(helpString);
            System.exit(1);
        } catch (IllegalArgumentException iae) {
            System.out.println("Error! Invalid argument -cs! Not a valid cache strategy!");
            System.out.println(helpString);
            System.exit(1);
        } catch (Exception e) {
            System.out.println("Unexpected error:\n" + e.getMessage());
            System.out.println(helpString);
            System.exit(1);
        }

        try {
            new LogSetup(logFile, logLevel);
            final KVServer server = new KVServer(port, cacheSize, strategy.toString(), address, directory);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    server.close();
                }
            });
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            System.exit(1);
        } catch (Exception e) {
            System.out.println("Unexpected error:\n" + e.getMessage());
            System.exit(1);
        }
    }
}
