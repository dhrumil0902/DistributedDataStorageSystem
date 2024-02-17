package ecs;

import app_kvServer.KVServer;

public class ECSNode implements IECSNode {
    private KVServer kvServer;
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private String[] nodeHashRange;
    private int cacheSize;
    private String dBStoragePath;
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

    public void startServer() {
        kvServer = new KVServer(nodePort, cacheSize, strategy, nodeHost, dBStoragePath);
    }
    public String getNodeName(){
        return null;
    }

    public String getNodeHost(){
        return null;
    }

    public int getNodePort(){
        return 0;
    }

    public String[] getNodeHashRange(){
        return null;
    }
}
