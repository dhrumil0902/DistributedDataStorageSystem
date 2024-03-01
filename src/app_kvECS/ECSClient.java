package app_kvECS;
import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;

import app_kvECS.ECSMessage.ActionType;
import app_kvECS.ServerConnection;
import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import shared.utils.HashUtils;
import shared.utils.HashUtils.*;
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

    public ECSClient(String address, int port) {

        nodes = new BST();
        this.port = port;
        this.address = address;
        startServer();

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
        logger.info("Initialize server ...here");
        try {
            logger.info("port" + port);
            InetSocketAddress socketAddress = new InetSocketAddress("127.0.0.1", port);
            logger.info("Initialize server ...here2");
            serverSocket = new ServerSocket();
            logger.info("Initialize server ...here3");
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
        ECSNode ecsNode = new ECSNode(port + "", address,port,hashRange, cacheSize, dBStoragePath, cacheStrategy);
        nodes.put(hashCode, ecsNode);
        if (nodes.size() > 1) {
            transferDataForNewNode(hashCode, getSuccessor(hashCode));
            ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
            successor.getNodeHashRange()[0] = hashCode;
        }
        return ecsNode;
    }

    public String putkeyValue(String key, String value){
        String hashedKey = HashUtils.getHash(key);
        if (nodes.isEmpty()){
            return null;
        }
        try {
            for (String nodeKey : nodes.sortedKeys()) {
                if (hashedKey.compareTo(nodeKey) <= 0) {
                    ECSNode ecsnode = (ECSNode) nodes.get(nodeKey);
                    Path filePathSuccessor = Paths.get(ecsnode.dBStoragePath, "data.txt");
                    BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(filePathSuccessor), true));
                    writer.write(key+ " " + value);
                    writer.newLine();
                    writer.close();
                    return key;
                }
            }
            ECSNode ecsnode = (ECSNode) nodes.get(nodes.min());
            Path filePathSuccessor = Paths.get(ecsnode.dBStoragePath, "data.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(filePathSuccessor), true));
            writer.write(key+ " " + value);
            writer.newLine();
            writer.close();
            return key;

        } catch (Exception e){
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
                String keyHashed = HashUtils.getHash(line.split(" ") [0]);
                if (maxRange.compareTo(minRange) > 0){
                    if (keyHashed.compareTo(minRange) > 0 && keyHashed.compareTo(maxRange) <= 0){
                        linesToTransfer.add(line);
                    }
                    else{
                        linesToKeep.add(line);
                    }
                }
                else{
                    if (keyHashed.compareTo(minRange) > 0 || keyHashed.compareTo(maxRange) <= 0){
                        linesToTransfer.add(line);
                    }
                    else{
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

    public String getStartNodeHash(String startingHash)
    {
        if (nodes.isEmpty()){
            return startingHash;
        }

        if (nodes.predecessor(startingHash) != null)
        {
            return nodes.predecessor(startingHash);
        }

        return nodes.max();
    }

    public String getSuccessor(String startingHash)
    {
        if (nodes.isEmpty()){
            return null;
        }

        if (nodes.successor(startingHash) != null)
        {
            return nodes.successor(startingHash);
        }

        return nodes.min();
    }

    public String getPredecessor(String startingHash)
    {
        if (nodes.isEmpty()){
            return null;
        }

        if (nodes.predecessor(startingHash) != null)
        {
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
        for (String nodeName : nodeNames){
            try {
                removeNode(nodeName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }
    public boolean removeNode(String nodeName) throws Exception {
        ECSNode removeNode = null;
        String removeNodeHash = null;
        for (String keyNode : nodes.keys()){
            ECSNode ecsNode = (ECSNode) nodes.get(keyNode);
            if (ecsNode.getNodeName().compareTo(nodeName) == 0){
                removeNode = ecsNode;
                removeNodeHash = keyNode;
            }
        }

        if (removeNode == null){
            return false;
        }

        if (nodes.size() == 1){
            ECSMessage msg = sendMessage(removeNode, new ECSMessage(ActionType.DELETE, true, null, null, nodes));
            nodes.delete(removeNodeHash);
            updateAllNodesMetaData();
            if (!msg.success){
                return false;
            }
            return true;
        }

        String hashOfSuccessor = getSuccessor(removeNodeHash);
        ECSNode successorNode = (ECSNode) nodes.get(hashOfSuccessor);
        transferDataForRemovedNode(removeNode, successorNode);
        successorNode.getNodeHashRange()[0] = removeNode.getNodeHashRange()[0];
        nodes.delete(removeNodeHash);
        updateAllNodesMetaData();
        return true;
    }

    private void transferDataForRemovedNode(ECSNode removeNode, ECSNode successorNode) {
        try{
            ECSMessage msg = sendMessage(removeNode, new ECSMessage(ActionType.GET_DATA, true, null, null, nodes));
            if (!msg.success){
                return;
            }
            msg.setAction(ActionType.APPEND);
            sendMessage(successorNode,msg);
            sendMessage(removeNode, new ECSMessage(ActionType.DELETE, true, null, null, nodes));

        }
        catch (Exception ex) {
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
        // TODO
        return null;
    }

    public ECSMessage onMessageReceived(String message, int port, String address) {
        if (message.compareTo("New Node") == 0) {
            String hashCode = HashUtils.getHash(address + ":" + port);
            String[] hashRange = {getStartNodeHash(hashCode), hashCode};
            ECSNode ecsNode = new ECSNode(address + port, address,port,hashRange);
            nodes.put(hashCode, ecsNode);
            List<String> kvToTransfer = new ArrayList<String>();
            if (nodes.size() > 1) {
                kvToTransfer = getKVPairsToTransfer(hashCode, getSuccessor(hashCode));
                ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
                successor.getNodeHashRange()[0] = hashCode;
                logger.info("Added new node to the bst, current state of bst: " + nodes.print());
                try {
                    logger.info("Transferring following data: " + kvToTransfer);
                    sendMessage(ecsNode, new ECSMessage(ActionType.SET_WRITE_LOCK, true, null,null, nodes));
                    sendMessage(ecsNode, new ECSMessage(ActionType.APPEND, true, kvToTransfer,null, nodes));
                    sendMessage(ecsNode, new ECSMessage(ActionType.UNSET_WRITE_LOCK, true, null,null, nodes));
                    sendMessage(successor, new ECSMessage(ActionType.REMOVE, true, null, hashRange, nodes));
                    sendMessage(successor, new ECSMessage(ActionType.UNSET_WRITE_LOCK, true, null,null, nodes));
                } catch (Exception e) {
                    logger.error(e);
                }
                updateAllNodesMetaData();
                return new ECSMessage(ActionType.UPDATE_METADATA, true, null,null, nodes);
            }
            logger.info("Added new node to the bst, current state of bst: " + nodes.print());
            updateAllNodesMetaData();
            return new ECSMessage(ActionType.UPDATE_METADATA, true,kvToTransfer,null, nodes);
        }
        updateAllNodesMetaData();
        return new ECSMessage(ActionType.APPEND, true,null,null, nodes);
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

    private List<String> addConnectedNewNode(int port, String address) {
        String hashCode = HashUtils.getHash(address + ":" + port);
        String[] hashRange = {getStartNodeHash(hashCode), hashCode};
        ECSNode ecsNode = new ECSNode(address + port, address,port,hashRange);
        nodes.put(hashCode, ecsNode);
        if (nodes.size() > 1) {
            List<String> kvToTransfer = getKVPairsToTransfer(hashCode, getSuccessor(hashCode));
            ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
            successor.getNodeHashRange()[0] = hashCode;
            return kvToTransfer;

        }
        return null;
    }
    private List<String> getKVPairsToTransfer(String newNode, String successor) {
        String minRange = getPredecessor(newNode); // Minimum value of the range
        String maxRange = newNode; // Maximum value of the range

        try {
            ECSNode ecsNodeSuccessor = (ECSNode) nodes.get(successor);
            ECSNode ecsNewNode = (ECSNode) nodes.get(newNode);
            ECSMessage ecsMessage = sendMessage(ecsNodeSuccessor, new ECSMessage(ActionType.SET_WRITE_LOCK, true,
                    null, new String[]{minRange, minRange}, nodes));
            if(!ecsMessage.success){
                logger.error("Received error while setting write lock on node: " + ecsNodeSuccessor.getNodeName() + ". Error: " + ecsMessage.getErrorMessage());
                return null;
            }
            logger.info("(In 'getKVPairsToTransfer'): Sending Message tp GET_DATA Keys to Node: " + ecsNodeSuccessor.getNodeName() + " range: " + minRange + "," + maxRange);
            ecsMessage = sendMessage(ecsNodeSuccessor, new ECSMessage(ActionType.GET_DATA, true,
                    null, new String[]{minRange, maxRange}, nodes));
            if (ecsMessage.success) {
                return ecsMessage.data;
            }
            return null;
        }
        catch (Exception ex){
            logger.error(ex);
        }
        return null;
    }

    public void updateAllNodesMetaData() {
        logger.info("Starting update of meta data of all nodes ...");
        for (ECSNode node : nodes.values())
        {
            try {
                sendMessage(node, new ECSMessage(ActionType.UPDATE_METADATA, true, null,null, nodes));
            } catch (Exception e) {
                logger.error("Could not successfully update the metadata of all nodes, error: " + e);
            }
        }
        logger.info("Finished updating meta data of all nodes ...");
    }
public ECSMessage sendMessage(ECSNode node, ECSMessage msg) throws Exception {
    try (Socket ECSSocket = new Socket(node.getNodeHost(), node.getNodePort())) {
        // Setup input and output streams
        ObjectOutputStream out = new ObjectOutputStream(ECSSocket.getOutputStream());
        ObjectInputStream in = new ObjectInputStream(ECSSocket.getInputStream());

        out.writeObject(msg);
        out.flush();

        // Wait for a response from the central server
        Object obj = in.readObject();
        if (obj instanceof ECSMessage) {
            return (ECSMessage) obj;
        }
    }catch(Exception ex){
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
        new LogSetup("test2.log", Level.ALL);
        new ECSClient("127.0.0.1", 5100);
        KVServer server = new KVServer("localhost", 5100, "localhost", 4710, 0, "None", System.getProperty("user.dir"));
        try {
            server.putKV("this", "val_test");
            server.putKV("dsdaslskdskldasklasclsalcss", "val_test");
            server.putKV("ewdfkdwloejwdflcdw", "val_test");
            server.putKV("ranyyyyyyyyyyyyyyyyyyyydom", "val_test");
            server.putKV("vfdfvfuuuuuuuuuuuuuuuuuuv", "val_test");
            server.putKV("dfvddfvkkuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu", "val_test");
            server.putKV("dvfdfvdfvdyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyf", "val_test");
            server.putKV("vdfvfffffffffffffffffffffffdfvdfv", "val_test");
            server.putKV("dfvddfvkkuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu", "val_test");
            server.putKV("dvfdfvdfvdyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyf", "val_test");
            server.putKV("vdfvfffffffffffffffffffffffdfvdfv", "val_test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //server = new KVServer("localhost", 5100, "localhost", 6700, 0, "None", System.getProperty("user.dir"));
        logger.info(server.getAllData());
        while(true){
        }
    }

}
