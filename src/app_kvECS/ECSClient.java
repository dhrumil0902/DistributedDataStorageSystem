package app_kvECS;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.io.*;
import app_kvECS.app_kvClient.KVClient;
import app_kvECS.app_kvClient.client.CommManager;
import app_kvECS.app_kvClient.client.KVStore;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import app_kvECS.app_kvClient.client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

public class ECSClient implements IECSClient {

    public BST nodes;
    UniqueRandomNumberGenerator randomNumberGen = new UniqueRandomNumberGenerator();

    public ECSClient() {
        nodes = new BST();
    }

    @Override
    public boolean start() {
        for (IECSNode node : nodes.values()){
            ECSNode escNode = (ECSNode) node;
            if (escNode.kvServer == null) {
                escNode.startServer();
            }
        }
        return true;
    }

    @Override
    public boolean stop() {
        for (IECSNode node : nodes.values()){
            ECSNode escNode = (ECSNode) node;
            escNode.kvServer.clearStorage();
            escNode.kvServer.close();
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    public ECSMessage SendMessage(ECSNode node, ECSMessage message){
        KVStore kvStore = new KVStore(node.getNodeHost(), node.getNodePort());
        try {
            kvStore.connect();
            String receivedMessage = kvStore.sendMessage(serializeToString(message));
            kvStore.disconnect();
            return deserializeFromString(receivedMessage);
        }
        catch (Exception ex) {
            kvStore.disconnect();
            System.out.println(ex.getMessage());
        }
        return null;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        String address = "127.0.0.1";
        int port = randomNumberGen.generateUniqueFourDigitNumber();
        String dBStoragePath = System.getProperty("user.dir") + "/" + port;
        String hashCode = getHash(address + ":" + port);
        String[] hashRange = {getStartNodeHash(hashCode), hashCode};
        ECSNode ecsNode = new ECSNode(port + "", address,port,hashRange, cacheSize, dBStoragePath, cacheStrategy);
        ecsNode.startServer();
        nodes.put(hashCode, ecsNode);
        if (nodes.size() > 1) {
            transferDataForNewNode(hashCode, getSuccessor(hashCode));
            ECSNode successor = (ECSNode) nodes.get(getSuccessor(hashCode));
            successor.nodeHashRange[0] = hashCode;
        }
        return ecsNode;
    }

    public String putkeyValue(String key, String value){
        String hashedKey = getHash(key);
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
                String keyHashed = getHash(line.split(" ") [0]);
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
        // TODO
        return false;
    }
    public boolean removeNode(String nodeName) {
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
            nodes.delete(removeNodeHash);
            return true;
        }
        String hashOfSuccessor = getSuccessor(removeNodeHash);
        ECSNode successorNode = (ECSNode) nodes.get(hashOfSuccessor);
        transferDataForRemovedNode(removeNode, successorNode);
        successorNode.nodeHashRange[0] = removeNode.nodeHashRange[0];
        try {
            Files.deleteIfExists(Paths.get(removeNode.dBStoragePath,"data.txt"));
            Files.delete(Paths.get(removeNode.dBStoragePath));
        }
        catch (Exception ex)  {
            System.out.println(Arrays.toString(ex.getStackTrace()));
        }
        nodes.delete(removeNodeHash);
        return true;
    }

    private void transferDataForRemovedNode(ECSNode removeNode, ECSNode successorNode) {
        Path filePathSuccessor = Paths.get(successorNode.dBStoragePath, "data.txt");
        Path filePathRemoveNode = Paths.get(removeNode.dBStoragePath, "data.txt");
        try{
            List<String> removeNodeLines = Files.readAllLines(filePathRemoveNode);

            BufferedWriter writer = new BufferedWriter(new FileWriter(String.valueOf(filePathSuccessor), true));
            for (String line : removeNodeLines) {
                writer.write(line);
                writer.newLine();
            }
            writer.close();

        }
        catch (Exception ex) {
            System.out.println(Arrays.toString(ex.getStackTrace()));
        }

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

    public String getHash(String key){
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes = md.digest(key.getBytes());
            return bytesToHex(md5Bytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(b & 0xFF);
            if (hex.length() == 1) {
                hexString.append('0'); // Add leading zero if necessary
            }
            hexString.append(hex);
        }
        return hexString.toString();
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
        Level Level;
        Level logLevel = org.apache.log4j.Level.ALL;
        IKVServer.CacheStrategy strategy = IKVServer.CacheStrategy.None;
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
                            logLevel = org.apache.log4j.Level.toLevel(args[i + 1]);
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
                        strategy = IKVServer.CacheStrategy.valueOf(args[i + 1]);
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
            new KVServer(port, cacheSize, strategy.toString(), address, directory);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            System.exit(1);
        } catch (Exception e) {
            System.out.println("Unexpected error:\n" + e.getMessage());
            System.exit(1);
        }
    }
}
