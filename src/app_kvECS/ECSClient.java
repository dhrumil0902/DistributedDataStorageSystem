package app_kvECS;

import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.ECSNode;
import ecs.IECSNode;
import shared.BST;
import shared.messages.ECSMessage;
import shared.messages.ECSMessage.ActionType;
import shared.utils.HashUtils;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ECSClient implements IECSClient, Runnable, Serializable {

    public BST nodes;
    private static Logger logger = Logger.getRootLogger();
    private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private String address;
    private List<ServerConnection> clientConnections = new ArrayList<ServerConnection>();
    UniqueRandomNumberGenerator randomNumberGen = new UniqueRandomNumberGenerator();
    private final Lock lock = new ReentrantLock();
    private Heartbeat heartbeat;

    public ECSClient(String address, int port) {

        nodes = new BST();
        this.port = port;
        this.address = address;
        startServer();
        new Thread(() -> {
            Heartbeat heartbeat = new Heartbeat(this);
            heartbeat.start();
        }).start();
    }

    @Override
    public void run() {
        this.running = initializeServer();
        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ServerConnection connection = new ServerConnection(client, this);
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

    private boolean isRunning() {
        return this.running;
    }

    private boolean initializeServer() {
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(address, port);
            serverSocket = new ServerSocket();
            serverSocket.bind(socketAddress);
            logger.info("ECSClient listening on port: " + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        String address = "127.0.0.1";
        int port = randomNumberGen.generateUniqueFourDigitNumber();
        String dBStoragePath = System.getProperty("user.dir") + "/" + port;
        String hashCode = HashUtils.getHash(address + ":" + port);
        String[] hashRange = {getStartNodeHash(hashCode), hashCode};
        ECSNode ecsNode = new ECSNode(port + "", address, port, hashRange, cacheSize, dBStoragePath, cacheStrategy);
        nodes.put(hashCode, ecsNode);
        if (nodes.size() > 1) {
            transferDataForNewNode(hashCode, getSuccessor(hashCode));
            ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
            successor.getNodeHashRange()[0] = hashCode;
        }
        return ecsNode;
    }

    public String putkeyValue(String key, String value) {
        String hashedKey = HashUtils.getHash(key);
        if (nodes.isEmpty()) {
            return null;
        }
        try {
            for (String nodeKey : nodes.sortedKeys()) {
                if (hashedKey.compareTo(nodeKey) <= 0) {
                    ECSNode ecsnode = (ECSNode) nodes.get(nodeKey);
                    Path filePathSuccessor = Paths.get(ecsnode.dBStoragePath, "data.txt");
                    BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(filePathSuccessor), true));
                    writer.write(key + " " + value);
                    writer.newLine();
                    writer.close();
                    return key;
                }
            }
            ECSNode ecsnode = (ECSNode) nodes.get(nodes.min());
            Path filePathSuccessor = Paths.get(ecsnode.dBStoragePath, "data.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(filePathSuccessor), true));
            writer.write(key + " " + value);
            writer.newLine();
            writer.close();
            return key;

        } catch (Exception e) {
            System.out.println(e.toString());
            return null;
        }
    }

    private void transferDataForNewNode(String newNode, String successor) {
        String minRange = getPredecessor(newNode); // Minimum value of the range
        String maxRange = newNode; // Maximum value of the range

        List<String> lines;
        List<String> linesToTransfer = new ArrayList<String>();
        List<String> linesToKeep = new ArrayList<String>();
        try {
            ECSNode ecsNodeSuccessor = (ECSNode) nodes.get(successor);
            ECSNode ecsNewNode = (ECSNode) nodes.get(newNode);
            Path filePathSuccessor = Paths.get(ecsNodeSuccessor.dBStoragePath, "data.txt");
            Path filePathNewNode = Paths.get(ecsNewNode.dBStoragePath, "data.txt");
            if (!Files.exists(filePathSuccessor)) {
                Files.createDirectories(filePathSuccessor.getParent());
                Files.createFile(filePathSuccessor);
            }
            if (!Files.exists(filePathNewNode)) {
                Files.createDirectories(filePathNewNode.getParent());
                Files.createFile(filePathNewNode);
            }

            lines = Files.readAllLines(filePathSuccessor);
            for (String line : lines) {
                System.out.println(line);
                String keyHashed = HashUtils.getHash(line.split(" ")[0]);
                if (maxRange.compareTo(minRange) > 0) {
                    if (keyHashed.compareTo(minRange) > 0 && keyHashed.compareTo(maxRange) <= 0) {
                        linesToTransfer.add(line);
                    } else {
                        linesToKeep.add(line);
                    }
                } else {
                    if (keyHashed.compareTo(minRange) > 0 || keyHashed.compareTo(maxRange) <= 0) {
                        linesToTransfer.add(line);
                    } else {
                        linesToKeep.add(line);
                    }
                }
            }

            BufferedWriter writerNodeNode = new BufferedWriter(new FileWriter(String.valueOf(filePathNewNode), false));
            for (String line : linesToTransfer) {
                writerNodeNode.write(line);
                writerNodeNode.newLine();
            }
            writerNodeNode.close();

            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(filePathSuccessor), false));
            for (String line : linesToKeep) {
                writer.write(line);
                writer.newLine();
            }
            writer.close();


        } catch (IOException e) {
            throw new RuntimeException("Failed to read from storage file: ", e);
        }
    }

    public String getStartNodeHash(String startingHash) {
        if (nodes.isEmpty()) {
            return startingHash;
        }

        if (nodes.predecessor(startingHash) != null) {
            return nodes.predecessor(startingHash);
        }

        return nodes.max();
    }

    public String getSuccessor(String startingHash) {
        if (nodes.isEmpty()) {
            return null;
        }

        if (nodes.successor(startingHash) != null) {
            return nodes.successor(startingHash);
        }

        return nodes.min();
    }

    public String getPredecessor(String startingHash) {
        if (nodes.isEmpty()) {
            return null;
        }

        if (nodes.predecessor(startingHash) != null) {
            return nodes.predecessor(startingHash);
        }

        return nodes.max();
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        //just for testing, disregard below code, but don't uncomment
        /*ECSNode node = new ECSNode("h","h", 890,null,9,"g", "k");
        nodes.put("this", node );
        Iterator<IECSNode> iterator = nodes.values().iterator();
        while (iterator.hasNext()) {
            IECSNode iecsNode = iterator.next();
            if (iecsNode instanceof ECSNode) {
                ECSNode node1 = (ECSNode) iecsNode;
                node1.startServer();
            }*/
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for (String nodeName : nodeNames) {
            try {
                //removeNode(nodeName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    public boolean removeNode(String nodeName, List<String> dataToTransfer) throws Exception {
        synchronized (lock) {
            ECSNode removeNode = null;
            String removeNodeHash = null;
            for (String keyNode : nodes.keys()) {
                ECSNode ecsNode = (ECSNode) nodes.get(keyNode);
                if (ecsNode.getNodeName().compareTo(nodeName) == 0) {
                    removeNode = ecsNode;
                    removeNodeHash = keyNode;
                }
            }

            if (removeNode == null) {
                return false;
            }

            if (nodes.size() == 1) {
                nodes.delete(removeNodeHash);
                return true;
            }

            String hashOfSuccessor = getSuccessor(removeNodeHash);
            ECSNode successorNode = (ECSNode) nodes.get(hashOfSuccessor);
            sendMessage(successorNode, new ECSMessage(ActionType.SET_WRITE_LOCK, true, null, null, nodes));
            sendMessage(successorNode, new ECSMessage(ActionType.APPEND, true, dataToTransfer, null, nodes));
            sendMessage(successorNode, new ECSMessage(ActionType.UNSET_WRITE_LOCK, true, null, null, nodes));
            successorNode.getNodeHashRange()[0] = removeNode.getNodeHashRange()[0];
            nodes.delete(removeNodeHash);
            updateAllNodesMetaData();
            logger.info("Removed a node from the bst, current state of bst: " + nodes.print());
            return true;
        }
    }

    private void transferDataForRemovedNode(ECSNode removeNode, ECSNode successorNode) {
        try {
            ECSMessage msg = sendMessage(removeNode, new ECSMessage(ActionType.TRANSFER, true, null, null, nodes));
            if (!msg.success) {
                return;
            }
            msg.setAction(ActionType.APPEND);
            sendMessage(successorNode, msg);
            sendMessage(removeNode, new ECSMessage(ActionType.DELETE, true, null, null, nodes));

        } catch (Exception ex) {
            System.out.println(Arrays.toString(ex.getStackTrace()));
        }

    }

    public void startServer() {
        new Thread(this).start();
    }


    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return nodes.get(Key);
    }

    public void onMessageReceived(String message, int port, String address) {
        synchronized (lock) {
            if (message.equals("New Node")) {
                String hashCode = HashUtils.getHash(address + ":" + port);
                String[] hashRange = {getStartNodeHash(hashCode), hashCode};
                ECSNode newNode = new ECSNode(address + ":" + port, address, port, hashRange);
                List<String> kvToTransfer = new ArrayList<String>();
                nodes.put(hashCode, newNode);
                if (nodes.size() > 1) {
                    ECSNode successorNode = (ECSNode) nodes.get(getSuccessor(hashCode));
                    successorNode.getNodeHashRange()[0] = hashCode;
                    if (dataTransfer(newNode, successorNode)) {
                        logger.info("Added new node to the bst, current state of bst: " + nodes.print());
                        updateAllNodesMetaData();
                    }
                }
                logger.info("Added new node to the bst, current state of bst: " + nodes.print());
                updateAllNodesMetaData();
            }
            logger.info("Unknown message type: " + message);
            updateAllNodesMetaData();
        }
    }

    //   public BST addNode(int port, String address) {
    //       String hashCode = getHash(address + ":" + port);
//            String[] hashRange = {getStartNodeHash(hashCode), hashCode};
//            ECSNode ecsNode = new ECSNode(address + port, address,port,hashRange);
//            nodes.put(hashCode, ecsNode);
//            List<String> kvToTransfer = new ArrayList<String>();
//            if (nodes.size() > 1) {
//                kvToTransfer = getKVPairsToTransfer(hashCode, getSuccessor(hashCode));
//                ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
//                successor.getNodeHashRange()[0] = hashCode;
//                logger.info(nodes.print());
//                return serializeToString(new ECSMessage(ActionType.APPEND, true, kvToTransfer,null, null));
//            }
//            logger.info("Added new node to the bst, current state of bst: " + nodes.print());
//            return serializeToString(new ECSMessage(ActionType.APPEND, true,kvToTransfer,null, null));
//        }
//        return serializeToString(new ECSMessage(ActionType.UPDATE_METADATA, true,null,null, null));
    //}

//    private List<String> addConnectedNewNode(int port, String address) {
//        String hashCode = HashUtils.getHash(address + ":" + port);
//        String[] hashRange = {getStartNodeHash(hashCode), hashCode};
//        ECSNode ecsNode = new ECSNode(address + ":" + port, address, port, hashRange);
//        nodes.put(hashCode, ecsNode);
//        if (nodes.size() > 1) {
//            List<String> kvToTransfer = getKVPairsToTransfer(hashCode, getSuccessor(hashCode));
//            ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
//            successor.getNodeHashRange()[0] = hashCode;
//            return kvToTransfer;
//
//        }
//        return null;
//    }

//    private List<String> getKVPairsToTransfer(String newNode, String successor) {
//        String minRange = getPredecessor(newNode); // Minimum value of the range
//        String maxRange = newNode; // Maximum value of the range
//
//        try {
//            ECSNode ecsNodeSuccessor = (ECSNode) nodes.get(successor);
//            ECSNode ecsNewNode = (ECSNode) nodes.get(newNode);
//            ECSMessage ecsMessage = sendMessage(ecsNodeSuccessor, new ECSMessage(ActionType.SET_WRITE_LOCK, true,
//                    null, new String[]{minRange, minRange}, nodes));
//            if (!ecsMessage.success) {
//                logger.error("Received error while setting write lock on node: " + ecsNodeSuccessor.getNodeName() + ". Error: " + ecsMessage.getErrorMessage());
//                return null;
//            }
//            logger.info("(In 'getKVPairsToTransfer'): Sending Message tp GET_DATA Keys to Node: " + ecsNodeSuccessor.getNodeName() + " range: " + minRange + "," + maxRange);
//            ecsMessage = sendMessage(ecsNodeSuccessor, new ECSMessage(ActionType.TRANSFER, true,
//                    null, new String[]{minRange, maxRange}, nodes));
//            if (ecsMessage.success) {
//                return ecsMessage.data;
//            }
//            return null;
//        } catch (Exception ex) {
//            logger.error(ex);
//        }
//        return null;
//    }

    private boolean dataTransfer(ECSNode newNode, ECSNode sucNode) {
        try {
            ECSMessage newNodeMsg = new ECSMessage();
            ECSMessage sucNodeMsg = new ECSMessage();
            newNodeMsg.setAction(ActionType.SET_WRITE_LOCK);
            sucNodeMsg.setAction(ActionType.SET_WRITE_LOCK);
            ECSMessage newNodeResponse = sendMessage(newNode, newNodeMsg);
            if (!newNodeResponse.success) {
                logger.error("Set write lock failed on: " + newNode.getNodeName());
                return false;
            }
            ECSMessage sucNodeResponse = sendMessage(sucNode, sucNodeMsg);
            if (!sucNodeResponse.success) {
                logger.error("Set write lock failed on: " + sucNode.getNodeName());
                return false;
            }
            sucNodeMsg.setAction(ActionType.TRANSFER);
            sucNodeMsg.setRange(newNode.nodeHashRange);
            sucNodeMsg.setServerInfo(newNode.getNodeHost(), newNode.getNodePort());
            sucNodeResponse = sendMessage(sucNode, sucNodeMsg);
            if (sucNodeResponse.getAction() == ActionType.TRANSFER & sucNodeResponse.success) {
                logger.info(String.format("Transfer data from %s to %s success.", sucNode.getNodeName(),
                        newNode.getNodeName()));
                // Unset write lock for new node
                newNodeMsg.setAction(ActionType.UNSET_WRITE_LOCK);
                sendMessage(newNode, newNodeMsg);
                // Remove transferred data from successor node
                sucNodeMsg.setAction(ActionType.REMOVE);
                sucNodeResponse = sendMessage(sucNode, sucNodeMsg);
                if (sucNodeResponse.getAction() == ActionType.REMOVE & sucNodeResponse.success) {
                    logger.info(String.format("Remove data from %s success.", sucNode.getNodeName()));
                    return true;
                } else {
                    logger.info(String.format("Remove data from %s failed.", sucNode.getNodeName()));
                    return false;
                }
            } else {
                logger.info(String.format("Transfer data from %s to %s failed.", sucNode.getNodeName(),
                        newNode.getNodeName()));
                return false;
            }
        } catch (Exception ex) {
            logger.error(ex);
            return false;
        }
    }

    public void updateAllNodesMetaData() {
        updateSuccessorAndPredecessorsInfo();
        logger.info("Starting update of meta data of all nodes ...");
        for (ECSNode node : nodes.values()) {
            try {
                sendMessage(node, new ECSMessage(ActionType.UPDATE_METADATA, true, null, null, nodes));
            } catch (Exception e) {
                logger.error("Could not successfully update the metadata of all nodes, error: " + e);
            }
        }
        logger.info("Finished updating meta data of all nodes ...");
    }

    private void updateSuccessorAndPredecessorsInfo() {
        if (nodes.isEmpty()){
            return;
        }
        Collection<ECSNode> currentNodes = nodes.values();
        for (ECSNode node : currentNodes) {
            try {
                node.predecessors = getPredecessorsList(node);
                node.successors = getSucessorsList(node);
            } catch (Exception e) {
                logger.error("Could not successfully update the successor and predecessor of all nodes, error: " + e);
            }
        }
        logger.info("Finished updating successors and predecessors data of all nodes ...");
    }

    private List<String> getPredecessorsList(ECSNode node) {
        List<String> predecessorsList = new ArrayList<>();
        if (nodes.size() == 1){
            return predecessorsList;
        }
        if (nodes.size() == 2){
            predecessorsList.add(getPredecessor(node.getNodeHashRange()[1]));
            return predecessorsList;
        }
        predecessorsList.add(getPredecessor(node.getNodeHashRange()[1]));
        predecessorsList.add(getPredecessor(predecessorsList.get(0)));
        return predecessorsList;
    }

    private List<String> getSucessorsList(ECSNode node) {
        List<String> getSuccessorsList = new ArrayList<>();
        if (nodes.size() == 1){
            return getSuccessorsList;
        }
        if (nodes.size() == 2){
            getSuccessorsList.add(getSuccessor(node.getNodeHashRange()[1]));
            return getSuccessorsList;
        }
        getSuccessorsList.add(getSuccessor(node.getNodeHashRange()[1]));
        getSuccessorsList.add(getSuccessor(getSuccessorsList.get(0)));
        return getSuccessorsList;
    }

    public ECSMessage sendMessage(ECSNode node, ECSMessage msg) throws Exception {
        try (Socket ECSSocket = new Socket(node.getNodeHost(), node.getNodePort())) {
            // Setup input and output streams
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ECSSocket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader in = new BufferedReader(new InputStreamReader(ECSSocket.getInputStream(), StandardCharsets.UTF_8));

            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(msg);

            logger.info("Send message: " + jsonString);

//            out.writeObject(msg);
            out.write(jsonString);
            out.newLine();
            out.flush();

            // Wait for a response from the central server
            String response = in.readLine();
            try {
                return new ObjectMapper().readValue(response, ECSMessage.class);
            } catch (JsonMappingException ex) {
                logger.error("Error during message deserialization.", ex);
            }
        } catch (Exception ex) {
            logger.error("While trying to receive/send message received error");
        }
        return null;
    }

    //    @Override
//    public void onConnectionClosed() {
//
//    }
    static ECSMessage deserializeFromString(String serializedString) {
        try {
            byte[] data = Base64.getDecoder().decode(serializedString);
            ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
            return (ECSMessage) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    static String serializeToString(Object obj) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(obj);
            objectOutputStream.close();
            return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        String helpString = generateHelpString();

        // Default Values
        int port = 5100;
        String address = "localhost";
        String logFile = "logs/ecsserver.log";
        Level logLevel = Level.ALL;

        if (args.length > 0 && args[0].equals("-h")) {
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
            final ECSClient escClient = new ECSClient(address, port);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    escClient.close();
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

    public void close() {
        try {
            serverSocket.close();
            logger.info("Successfully closed ECSClient");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String generateHelpString() {
        return "Help Screen:"
                + "Options:\n"
                + "  -p <port>          Port number for the KVServer (default: 5000)\n"
                + "  -a <address>       Address for the KVServer (default: localhost)\n"
                + "  -l <logFile>       File path for the log file (default: ./server.log)\n"
                + "  -ll <logLevel>     Log level for the server (default: ALL)\n"
                + "Example:\n"
                + "  java KVServer -p 8080 -a 127.0.0.1  -l /path/to/server.log -ll INFO";
    }

    public void onServerDown(ECSNode node) {
        logger.info("Server" + node.getNodeName() + "is down.");
    }


    public void sendHeartbeats() {
        synchronized (this) {
            logger.info("Sending HeartBeats");
            /*for (IECSNode node : this.getNodes().values()) {
                try {
                    ECSMessage heartbeatMsg = new ECSMessage(ActionType.HEARTBEAT, true, null, null, null);
                    ECSMessage response = this.sendMessage((ECSNode) node, heartbeatMsg);
                    if (response == null || !response.success) {
                        logger.info("Failed to receive heartbeat response from: " + node.getNodeName());
                    } else {
                        logger.info("Received heartbeat response from: " + node.getNodeName());
                    }
                } catch (Exception e) {
                    logger.error("Error sending heartbeat to " + node.getNodeName());
                    e.printStackTrace();
                }
            }*/
            }
        }
    }

