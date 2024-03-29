package ecs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ECSNode implements IECSNode, Serializable {
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    public String[] nodeHashRange;
    private int cacheSize;
    public String dBStoragePath;
    private String strategy;
    public List<String> predecessors = new ArrayList<>(2);
    public List<String> successors = new ArrayList<>(2);

    public ECSNode() {}

    public ECSNode(String nodeName, String nodeHost, int nodePort, String[] nodeHashRange, int cacheSize, String dBStoragePath,
                   String strategy) {
        this.nodeName = nodeHost + ":" + nodePort;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.nodeHashRange = nodeHashRange;
        this.cacheSize = cacheSize;
        this.dBStoragePath = dBStoragePath;
        this.strategy = strategy;
    }

    public ECSNode(String nodeName, String address, int port, String[] hashRange) {
        this.nodeName = address + ":" + port;
        this.nodeHost = address;
        this.nodePort = port;
        this.nodeHashRange = hashRange;
    }

    public void startServer() {
    }
    public String getNodeName(){
        return this.nodeName;
    }

    public String getNodeHost(){
        return this.nodeHost;
    }

    public int getNodePort(){
        return this.nodePort;
    }

    public String[] getNodeHashRange(){
        return nodeHashRange;
    }

    public List<String> getPredecessors() {return predecessors;}

    public List<String> getSuccessors() {return successors;}

    public void setSuccessors(List<String> newSuccessors) {
        if (newSuccessors == null) {
            throw new IllegalArgumentException("Successor list cannot be null.");
        }
        if (newSuccessors.size() > 2) {
            throw new IllegalArgumentException("Successor list cannot contain more than two elements.");
        }

        // Clear the current successors list and add all from the new list
        this.successors.clear();
        this.successors.addAll(newSuccessors);
    }
}
