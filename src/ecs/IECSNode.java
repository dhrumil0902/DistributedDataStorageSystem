package ecs;
import java.util.List;

public interface IECSNode {

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

    /**
     * @return  the list of the names of the predecessor nodes
     */
    public List<String> getPredecessors();

    /**
     * @return  the list of the names of the successor nodes
     */
    public List<String> getSuccessors();

}
