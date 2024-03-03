package app_kvServer;

import static shared.messages.ECSMessage.ActionType;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

import app_kvServer.kvCache.FIFOCache;
import app_kvServer.kvCache.IKVCache;
import app_kvServer.kvCache.LFUCache;
import app_kvServer.kvCache.LRUCache;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import shared.BST;
import shared.messages.ECSMessage;
import shared.messages.ECSMessage.ActionType;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageImpl;
import shared.utils.HashUtils;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;


import java.util.Map;

public class KVServer implements IKVServer, Runnable {
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
    private static Logger logger = Logger.getRootLogger();
    private String address;
    private int port;
    private String ecsAddress;
    private int ecsPort;
    int cacheSize;
    CacheStrategy strategy;
    private ServerSocket serverSocket;
    private boolean running;
    private boolean register;
    private String storagePath;
    private IKVCache cache;
    private KVStorage storage;
    private final Object lock = new Object();
    private BST metadata;
    private boolean writeLock;
    private List<ClientConnection> clientConnections = new ArrayList<ClientConnection>();

//    public KVServer(int port, int cacheSize, String strategy) {
//        this(port, cacheSize, strategy, "localhost", System.getProperty("user.dir"));
//    }

    public KVServer(String ecsAddress, int ecsPort, String address, int port, int cacheSize, String strategy,
                    String storageDir) {
        String fileName = address + "_" + port + ".txt";
        this.storagePath = storageDir + File.separator + fileName;
        this.ecsAddress = ecsAddress;
        this.ecsPort = ecsPort;
        this.address = address;
        this.port = port;
        this.cacheSize = cacheSize;
        this.writeLock = false;
        this.metadata = null;
        this.register = false;
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
        return port;
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

    public String getStoragePath() {
        return this.storagePath;
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
        logger.info("Storage dir: " + getStoragePath());
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

    public boolean deleteKV(String key) throws Exception {
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

    public void updateStorage(String key, String value) throws Exception {
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

    public void appendDataToStorage(List<String> data) {
        storage.putList(data);
    }

    public List<String> getAllData() {
        try {
            return storage.getAllData();
        } catch (IOException e) {
            logger.error("Unable to retrieve data from storage", e);
            return null;
        }
    }

    public List<String> getData(String minVal, String maxVal) {
        try {
            return storage.getData(minVal, maxVal);
        } catch (IOException e) {
            logger.error("Unable to retrieve data from storage", e);
            return null;
        }
    }

    public boolean removeData(String minVal, String maxVal) {
        logger.info("In removeData function in: " + port);
        try {
            storage.removeData(minVal, maxVal);
            return true;
        } catch (IOException e) {
            logger.error("Unable to remove data from storage", e);
            return false;
        }
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

    private void writeEvictedToStorage(String key, String value) throws RuntimeException {
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
        new Thread(new Runnable() {
            @Override
            public void run() {
                connectToCentralServer();
            }
        }).start();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client, this);
                    clientConnections.add(connection);
                    new Thread(connection).start();

                    logger.info(
                            "From Server: Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
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
            disconnectFromCentralServer();
            serverSocket.close();
            for (ClientConnection connection : clientConnections) {
                connection.close();
            }
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(address, port);
            serverSocket = new ServerSocket();
            serverSocket.bind(socketAddress);
            logger.info("Server listening on port: " + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    private boolean isRunning() {
        return this.running;
    }

    private void connectToCentralServer() {
//        if (ECSAddress.isEmpty() || ecsPort == -1) {
//            return;
//        }
        try (Socket ECSSocket = new Socket(ecsAddress, ecsPort)) {
            // Setup input and output streams
            logger.info("Send connection request to ECS.");
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ECSSocket.getOutputStream(), StandardCharsets.UTF_8));
            ECSMessage msg = new ECSMessage();
            msg.setAction(ActionType.NEW_NODE);
            msg.setServerInfo(address, port);

            ObjectMapper mapper = new ObjectMapper();
            try {
                String jsonString = mapper.writeValueAsString(msg);
                out.write(jsonString);
                out.newLine();
                out.flush();
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse ECSMessage.");
            } catch (IOException e) {
                logger.error("Failed to send ECSMessage.");
            }
        } catch (IOException e) {
            logger.error("Failed to connect to the central server.", e);
        }
    }

    private void disconnectFromCentralServer() {

        try (Socket ECSSocket = new Socket(ecsAddress, ecsPort)) {
            // Setup input and output streams
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ECSSocket.getOutputStream(), StandardCharsets.UTF_8));
            ECSMessage msg = new ECSMessage();
            msg.setData(getAllData());
            msg.setAction(ActionType.DELETE);
            msg.setServerInfo(address, port);
            if (metadata.size() > 1){
                logger.info("Removing all the data from: " + port);
                removeData("00000000000000000000000000000000","FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
            }
            ObjectMapper mapper = new ObjectMapper();
            try {
                String jsonString = mapper.writeValueAsString(msg);
                out.write(jsonString);
                out.newLine();
                out.flush();
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse ECSMessage.");
            } catch (IOException e) {
                logger.error("Failed to send ECSMessage.");
            }
            logger.info("Informing ECSClient to delete server: " + port);
        } catch (IOException e) {
            logger.error("Failed to connect to the central server.", e);
        }
    }


    //    @Override
//    public String onMessageReceived(String message) {
//        if (message == null || message.isEmpty()) {
//            logger.error("SERVER: Received empty or null message from client.");
//            return "FAILED Received empty or null message from client";
//        }
//
//        logger.info("SERVER: Received message from client: " + message);
//        logger.info("SERVER: Parsing the message on the server side ...");
//
//        String[] splitMessage = message.split(" ");
//
//        if (splitMessage.length == 0) {
//            logger.error("Invalid empty message received.");
//            return "FAILED Invalid empty message received.";
//        }
//
//        String command = splitMessage[0];
//
//        switch (command) {
//            case "get":
//                return handleGetMessage(splitMessage);
//
//            case "put":
//                return handlePutMessage(splitMessage);
//
//            default:
//                logger.error("Unknown command: " + command);
//                return "FAILED unknown command " + command;
//        }
//    }

    public boolean checkKeyRange(String key) {
        String nodeHash = HashUtils.getHash(address + ":" + port);
        String[] keyRange = metadata.get(nodeHash).getNodeHashRange();
        return HashUtils.evaluateKeyHash(key, keyRange[0], keyRange[1]);
    }

    public KVMessage handleGetMessage(KVMessage message) {
        String key = message.getKey();
        KVMessage response = new KVMessageImpl();
        if (checkKeyRange(key)) {
            response.setKey(key);
            synchronized (lock) {
                try {
                    logger.info("SERVER: Trying to GET the value associated with Key '" + key);
                    String value = getKV(key);
                    if (value == null || value.isEmpty()) {
                        response.setStatus(StatusType.GET_ERROR);
                        return response;
                    }
                    response.setStatus(StatusType.GET_SUCCESS);
                    response.setValue(value);
                } catch (Exception e) {
                    logger.error("Error retrieving value for key '" + key + "': " + e.getMessage());
                    response.setStatus(StatusType.GET_ERROR);
                    return response;
                }
            }
        } else {
            response.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
            response.setMetadata(this.metadata);
        }
        return response;

    }

    public KVMessage handlePutMessage(KVMessage message) {
        String key = message.getKey();
        KVMessage response = new KVMessageImpl();
        if (getWriteLock()) {
            response.setStatus(StatusType.SERVER_WRITE_LOCK);
            return response;
        }
        if (checkKeyRange(key)) {
            response.setKey(key);
            synchronized (lock) {
                logger.debug("Got the Lock");
                String value = message.getValue();
                // Delete
                if (value.equals("null")) {
                    try {
                        logger.info("SERVER: Key '" + key + "' deleted '");
                        if (!deleteKV(key)) {
                            logger.info(String.format("SERVER: Delete key %s not exists.", key));
                            response.setStatus(StatusType.DELETE_ERROR);
                            return response;
                        }
                        response.setStatus(StatusType.DELETE_SUCCESS);
                        return response;
                    } catch (Exception e) {
                        logger.error("Error deleting key '" + key + " " + e.getMessage());
                        response.setStatus(StatusType.DELETE_ERROR);
                        return response;
                    }
                }
                response.setValue(value);
                // Update
                if (inCache(key)) {
                    logger.info("SERVER: Update cache with Key '" + key + "', Value '" + value + "'");
                    cache.updateKV(key, value);
                    response.setStatus(StatusType.PUT_UPDATE);
                    return response;
                }
                if (inStorage(key)) {
                    logger.info("SERVER: Update storage with Key '" + key + "', Value '" + value + "'");
                    try {
                        updateStorage(key, value);
                        response.setStatus(StatusType.PUT_UPDATE);
                        return response;
                    } catch (Exception e) {
                        logger.error("Error updating key-value pair for key '" + key + "': " + e.getMessage());
                        response.setStatus(StatusType.PUT_ERROR);
                        return response;
                    }
                }

                //Put
                logger.info("SERVER: PUT  Key '" + key + "', Value '" + value + "'");
                try {
                    putKV(key, value);
                    response.setStatus(StatusType.PUT_SUCCESS);
                } catch (Exception e) {
                    logger.error("Error putting key-value pair for key '" + key + "': " + e.toString());
                    response.setStatus(StatusType.PUT_ERROR);
                    return response;
                }
            }
        } else {
            response.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
        }
        return response;
    }

    public KVMessage handleKeyRangeMessage(KVMessage msg) {
        KVMessage message = new KVMessageImpl();
        message.setMetadata(metadata);
        if (message.getMetadata() == null) {
            message.setStatus(KVMessage.StatusType.KEYRANGE_ERROR);
        } else {
            message.setStatus(KVMessage.StatusType.KEYRANGE_SUCCESS);
        }
        return message;
    }

//    @Override
//    public void onConnectionClosed() {
//
//        logger.info("Connection closed by client.");
//    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void updateMetadata(BST metadata) {
        if (this.metadata == null) {
            logger.info("Register to ECS success.");
            this.metadata = metadata;
            this.register = true;
            return;
        }
        this.metadata = metadata;
    }

    public boolean getWriteLock() {
        return this.writeLock;
    }

    public void setWriteLock(boolean flag) {
        this.writeLock = flag;
    }

    public BST getMetadata() {
        return this.metadata;
    }

    public boolean checkRegisterStatus() {return this.register;}

    private static String generateHelpString() {
        return "Usage: java KVServer [-p <port>] [-a <address>] [-d <directory>] [-l <logFile>] [-ll <logLevel>] [-c <cacheSize>] [-cs <cacheStrategy>]\n"
                + "Options:\n"
                + "  -b <address:port>  Address and port number of the ECS server (default: localhost:5001)\n"
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
        int ecsPort = 5100;
        String ecsAddress = "localhost";
        String directory = System.getProperty("user.dir");
        String logFile = String.format("logs/%s_%d.log", address, port);
        Level logLevel = Level.ALL;
        CacheStrategy strategy = CacheStrategy.None;
        int cacheSize = 10;

        if (args.length > 0 && args[0].equals("-h")) {
            System.out.println(helpString);
            System.exit(1);
        }

//        if (args.length % 2 != 0) {
//            System.out.println("Invalid number of arguments");
//            System.out.println(helpString);
//            System.exit(1);
//        }

        try {
            for (int i = 0; i < args.length; i += 2) {
                switch (args[i]) {
                    case "-b":
                        String[] pair = args[i + 1].split(":");
                        ecsAddress = pair[0];
                        ecsPort = Integer.parseInt(pair[1]);
                        break;
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
            final KVServer server = new KVServer(ecsAddress, ecsPort, address, port, cacheSize, strategy.toString(), directory);
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
