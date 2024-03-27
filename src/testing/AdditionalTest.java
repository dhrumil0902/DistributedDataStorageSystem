package testing;

import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import app_kvECS.ECSClient;
import junit.framework.TestCase;
import shared.BST;
import shared.messages.CoordMessage;
import shared.utils.HashUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AdditionalTest extends TestCase {

    @Test
    public void testByteToHex() {
        byte[] byteArray = {0x0A, 0x1F, 0x2B, (byte) 0xFF};
        String expectedResult = "0a1f2bff";
        String actualResult = HashUtils.bytesToHex(byteArray);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGetHash() {
        String key = "testKey";
        String expectedResult = "24afda34e3f74e54b61a8e4cbe921650";
        String actualResult = HashUtils.getHash(key);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testCheckHashRangeofOneServer() {
        try {
            new LogSetup("test2.log", Level.ALL);
            ECSClient client = new ECSClient("localhost", 5101);
            KVServer server = new KVServer("localhost", 5101, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            assertEquals(1, client.nodes.size());
            client.nodes.values().iterator().next().getNodeHashRange();
            assertEquals(client.nodes.values().iterator().next().getNodeHashRange()[0], client.nodes.values().iterator().next().getNodeHashRange()[1]);
            server.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testHashRangeUpdateForTwoServers() {
        try {
            new LogSetup("test2.log", Level.ALL);
            ECSClient client = new ECSClient("localhost", 5110);
            KVServer server = new KVServer("localhost", 5110, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
            KVServer server2 = new KVServer("localhost", 5110, "localhost", 5717, 0, "None", System.getProperty("user.dir"));

            Thread.sleep(1000);
            assertEquals(2, client.nodes.size());
            client.nodes.values().iterator().next().getNodeHashRange();
            assertEquals(client.getNodeByKey(client.nodes.min()).getNodeHashRange()[0], client.getNodeByKey(client.nodes.max()).getNodeHashRange()[1]);
            server.close();
            Thread.sleep(1000);
            server2.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testHashRangeUpdateOnServerDeletion() {
        try {
            new LogSetup("test2.log", Level.ALL);
            ECSClient client = new ECSClient("localhost", 5111);
            KVServer server = new KVServer("localhost", 5111, "localhost", 5810, 0, "None", System.getProperty("user.dir"));
            KVServer server2 = new KVServer("localhost", 5111, "localhost", 5817, 0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            server.close();
            Thread.sleep(1000);
            assertEquals(1, client.nodes.size());
            client.nodes.values().iterator().next().getNodeHashRange();
            assertEquals(client.getNodeByKey(client.nodes.min()).getNodeHashRange()[0], client.getNodeByKey(client.nodes.min()).getNodeHashRange()[1]);
            Thread.sleep(1000);
            server2.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testConnectingServerToECSCleint() {
        try {
            new LogSetup("test2.log", Level.ALL);
            ECSClient client = new ECSClient("localhost", 5201);
            KVServer server = new KVServer("localhost", 5201, "localhost", 5910, 0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            assertEquals(1, client.nodes.size());
            server.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAddingTwoServersToECSCleint() {
        try {
            new LogSetup("test2.log", Level.ALL);
            System.out.println("here");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ECSClient client = new ECSClient("localhost", 5100);
        KVServer server = new KVServer("localhost", 5100, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
        KVServer server2 = new KVServer("localhost", 5100, "localhost", 5711, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(2, client.nodes.size());
    }

    @Test
    public void testTransferOfData() {
        try {
            new LogSetup("test2.log", Level.ALL);
            System.out.println("here");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ECSClient client = new ECSClient("localhost", 5103);
        KVServer server = new KVServer("localhost", 5103, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
        try {
            server.putKV("testkey", "testvalue");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        KVServer server2 = new KVServer("localhost", 5103, "localhost", 5711, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        server.close();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            assertEquals("testvalue", server2.getKV("testkey"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemovingServersFromECSCleint() {
        try {
            new LogSetup("test2.log", Level.ALL);
            System.out.println("here");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ECSClient client = new ECSClient("localhost", 5105);
        KVServer server = new KVServer("localhost", 5105, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
        KVServer server2 = new KVServer("localhost", 5105, "localhost", 5711, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(1000);
            assertEquals(2, client.nodes.size());
            server.close();
            Thread.sleep(1000);
            assertEquals(1, client.nodes.size());
            server2.close();
            Thread.sleep(1000);
            assertEquals(0, client.nodes.size());
        }
        catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
    }
    @Test
    public void testPersistentStorage() {
        try {
            new LogSetup("test2.log", Level.ALL);
            System.out.println("here");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ECSClient client = new ECSClient("localhost", 5408);
        KVServer server = new KVServer("localhost", 5408, "localhost", 5716, 0, "None", System.getProperty("user.dir"));
        try {
            server.putKV("testkey", "testvalue");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        KVServer server2 = new KVServer("localhost", 5408, "localhost", 5715, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        server.close();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        server.removeData("00000000000000000000000000000000","FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            assertEquals("testvalue", server2.getKV("testkey"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        server2.close();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        server2 = new KVServer("localhost", 5108, "localhost", 5711, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            assertEquals("testvalue", server2.getKV("testkey"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTempTest() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(2000);
            server0.putKV("key", "val");
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 42157, 0, "None", System.getProperty("user.dir"));
            KVServer server3 = new KVServer("localhost", 5100, "localhost", 38977, 0, "None", System.getProperty("user.dir"));
            KVServer server4 = new KVServer("localhost", 5100, "localhost", 44791, 0, "None", System.getProperty("user.dir"));
//            System.out.println(server0.getMetadata().print());
            Thread.sleep(3000);
            System.out.println(server2.getKV("key"));
            System.out.println(server0.getKV("key"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testUpdateReplica() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(2000);
            server0.putKV("key", "val");
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(2000);
            BST metadata = server0.getMetadata();
            ECSNode server0Node = (ECSNode) metadata.get(server0.getHashValue());
            ECSNode server1Node = (ECSNode) metadata.get(server1.getHashValue());
            List<ECSNode> successors = Arrays.asList(server1Node);
            server0Node.setSuccessors(successors);
            server0.setReplications(server0Node);
            CoordMessage message = new CoordMessage();
            message.setAction(CoordMessage.ActionType.PUT);
            message.setKey("key");
            message.setValue("val");
            server0.updateReplica(message);
            Thread.sleep(2000);
            System.out.println(server0.getMetadata().print());
            System.out.println(server0.getKV("key"));
            System.out.println(server1.getKV("key"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
