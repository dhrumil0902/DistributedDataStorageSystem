package ecs;

import java.io.Serializable;

public class ECSNode implements IECSNode, Serializable {
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    public String[] nodeHashRange;
    private int cacheSize;
    public String dBStoragePath;
    private String strategy;


    public ECSNode(String nodeName, String nodeHost, int nodePort, String[] nodeHashRange, int cacheSize, String dBStoragePath, String strategy) {
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.nodeHashRange = nodeHashRange;
        this.cacheSize = cacheSize;
        this.dBStoragePath = dBStoragePath;
        this.strategy = strategy;
    }

    public ECSNode(String nodeName, String address, int port, String[] hashRange) {
        this.nodeName = nodeName;
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

}
