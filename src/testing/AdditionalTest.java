package testing;

import ecs.ECSNode;
import ecs.IECSNode;
import org.junit.Test;
import app_kvECS.ECSClient;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class AdditionalTest extends TestCase {

	@Test
    public void testBytesToHex() {
        byte[] byteArray = {0x0A, 0x1F, 0x2B, (byte) 0xFF}; // Example byte array
         ECSClient client = new ECSClient("t", 6);
         String temp = client.bytesToHex(byteArray);

         String c1 = "0b1";
         String c2 = "0b2";

         if (c1.compareTo(c2) > 0)
         {
             System.out.println(c1 + " is greater");
         }
         else{
             System.out.println(c2 + " is greater");
        }
        String temp1 = client.getHash("127.8.9:5500");
    }

        @Test
        public void testAddNode() {
            ECSClient client = new ECSClient("t", 6);
            client.addNode("temp", 56);
            client.addNode("temp", 56);
            client.addNode("temp", 56);
            client.putkeyValue("TEST KEY", "THIS IS THE VALUE");
            client.putkeyValue("zebra", "THIS IS THE VALUE");
            client.putkeyValue("giraffe", "THIS IS THE VALUE");
            client.putkeyValue("kangaroo", "THIS IS THE VALUE");
            client.putkeyValue("god", "THIS IS THE VALUE");
            client.putkeyValue("bat", "THIS IS THE VALUE");
            client.putkeyValue("fvdfvdfv", "THIS IS THE VALUE");
            client.addNode("temp", 56);

            List<String> keysToRemove = new ArrayList<>();
            for (String hashKey : client.nodes.keys()) {
                keysToRemove.add(hashKey);
            }

            for (String hashKey : keysToRemove) {
                IECSNode node = client.nodes.get(hashKey);
                if (node instanceof ECSNode) {
                    ECSNode ecsNode = (ECSNode) node;
                    client.removeNode(ecsNode.getNodeName());
                } else {
                    System.err.println("Error: Unexpected node type encountered for key: " + hashKey);
                }
            }
        }
        @Test
        public void testGetNodeHashStartRange() {
            ECSClient client = new ECSClient("7",7);
            client.nodes.put("0000000000000600000000001", null);
            String tmep = client.getStartNodeHash("0070000000000600000000001");

            client.nodes.put("0006000000000000000200000", null);
            tmep = client.getStartNodeHash("0000000000000000000000001");
            client.nodes.put("0000000000000000000000300", null);
            tmep = client.getStartNodeHash("0000000000000000000000001");
            client.nodes.put("0000004000000000000000000", null);
            tmep = client.getStartNodeHash("0000000000000000000000001");
            client.nodes.put("0500000000000000000000001", null);
            tmep = client.getStartNodeHash("0000000000000000000000001");
            client.addNode("temp",56);
        }

}
