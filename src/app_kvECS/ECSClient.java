package app_kvECS;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;

import ecs.ECSNode;
import ecs.IECSNode;
public class ECSClient implements IECSClient {

    public BST nodes;
    UniqueRandomNumberGenerator randomNumberGen = new UniqueRandomNumberGenerator();

    public ECSClient() {
        nodes = new BST();
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
        String hashCode = getHash(address + ":" + port);
        String[] hashRange = {getStartNodeHash(hashCode), hashCode};
        ECSNode ecsNode = new ECSNode(port + "", address,port,hashRange, cacheSize, dBStoragePath, cacheStrategy);
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

    public static void main(String[] args) {
        // TODO
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
}
