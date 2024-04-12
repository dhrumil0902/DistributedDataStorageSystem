package testing;

import client.KVStore;
import shared.Heartbeat;
import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import app_kvECS.ECSClient;
import junit.framework.TestCase;
import shared.messages.CoordMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.utils.HashUtils;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


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
    public void ensureReplicaUpdateReplicaRequestSent() {
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
    public void testAddCoordAndReplica() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            server0.putKV("key", "val0");
            server0.putKV("abc", "val1");
            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);

            KVMessage query1 = new KVMessageImpl();
            KVMessage query2 = new KVMessageImpl();
            query1.setKey("key");
            query2.setKey("abc");
            query1.setStatus(KVMessage.StatusType.GET);
            query2.setStatus(KVMessage.StatusType.GET);
            KVMessage response = server0.handleGetMessage(query1);
            assertEquals(response.getValue(), "val0");
            response = server1.handleGetMessage(query1);
            assertEquals(response.getValue(), "val0");
            response = server0.handleGetMessage(query2);
            assertEquals(response.getValue(), "val1");
            response = server1.handleGetMessage(query2);
            assertEquals(response.getValue(), "val1");
            Thread.sleep(1000);

            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);
            response = server2.handleGetMessage(query1);
            assertEquals(response.getValue(), "val0");
            response = server2.handleGetMessage(query2);
            assertEquals(response.getValue(), "val1");
//            Thread.sleep(5000);

            server2.close();
            Thread.sleep(3000);
            response = server0.handleGetMessage(query2);
            assertEquals(response.getValue(), "val1");
            response = server1.handleGetMessage(query2);
            assertEquals(response.getValue(), "val1");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testPutOperation() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
//            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
//            Thread.sleep(1000);
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);
            KVMessage query = new KVMessageImpl();
            query.setStatus(KVMessage.StatusType.PUT);
            query.setKey("key");
            query.setValue("val0");
            KVMessage response = server1.handlePutMessage(query);
            System.out.println(response.getStatus().toString());
            Thread.sleep(3000);
            CoordMessage syncMessage = new CoordMessage(server1.getHashValue());
            syncMessage.setData(server1.getAllData());
            syncMessage.setAction(CoordMessage.ActionType.FORCE_SYNC);
            server1.updateReplica(syncMessage);
            Thread.sleep(2000);
            query.setStatus(KVMessage.StatusType.GET);
            response = server0.handleGetMessage(query);
            assertEquals(response.getValue(), "val0");
            response = server1.handleGetMessage(query);
            assertEquals(response.getValue(), "val0");
            response = server2.handleGetMessage(query);
            assertEquals(response.getValue(), "val0");
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testUpdateOperation() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
//            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
//            Thread.sleep(1000);
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(2000);

            KVMessage query = new KVMessageImpl();
            query.setStatus(KVMessage.StatusType.PUT);
            query.setKey("key");
            query.setValue("val0");
            KVMessage response = server1.handlePutMessage(query);
            System.out.println(response.getStatus().toString());
            Thread.sleep(3000);
            CoordMessage syncMessage = new CoordMessage(server1.getHashValue());
            syncMessage.setData(server1.getAllData());
            syncMessage.setAction(CoordMessage.ActionType.FORCE_SYNC);
            server1.updateReplica(syncMessage);
            Thread.sleep(2000);
            query.setStatus(KVMessage.StatusType.GET);
            response = server0.handleGetMessage(query);
            Thread.sleep(2000);
            assertEquals(response.getValue(), "val0");
            response = server1.handleGetMessage(query);
            assertEquals(response.getValue(), "val0");
            response = server2.handleGetMessage(query);
            assertEquals(response.getValue(), "val0");
            Thread.sleep(1000);
            KVMessage query1 = new KVMessageImpl();
            query1.setStatus(KVMessage.StatusType.PUT);
            query1.setKey("key");
            query1.setValue("val1");
            response = server1.handlePutMessage(query1);
            System.out.println(response.getStatus().toString());
            Thread.sleep(3000);
            syncMessage = new CoordMessage(server1.getHashValue());
            syncMessage.setData(server1.getAllData());
            syncMessage.setAction(CoordMessage.ActionType.FORCE_SYNC);
            server1.updateReplica(syncMessage);
            Thread.sleep(2000);
            query1.setStatus(KVMessage.StatusType.GET);
            response = server1.handleGetMessage(query);
            assertEquals(response.getValue(), "val1");
            response = server0.handleGetMessage(query);
            assertEquals(response.getValue(), "val1");
            response = server2.handleGetMessage(query);
            assertEquals(response.getValue(), "val1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testDeleteOperation() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);
            KVMessage query = new KVMessageImpl();
            query.setStatus(KVMessage.StatusType.PUT);
            query.setKey("key");
            query.setValue("null");
            KVMessage response = server1.handlePutMessage(query);
            assertNull(response.getValue());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testKeyrangeMsg() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testDirectPutToReplica() {
        try {
            new LogSetup("test3.log", Level.ALL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Simulate an ECS client and two servers (one primary, one replica)
        ECSClient client = new ECSClient("localhost", 5100);
        KVServer primaryServer = new KVServer("localhost", 5100, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        KVServer replicaServer = new KVServer("localhost", 5100, "localhost", 5711, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Attempt to perform a PUT operation directly on the replica server
        try {
            replicaServer.putKV("bcvbcvbcvbcvb", "testValue");

        } catch (Exception e) {
            // If an exception is thrown (expected behavior), pass the test
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void testUnExpectedShutdown() {
        try {
            new LogSetup("test3.log", Level.ALL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Simulate an ECS client and two servers (one primary, one replica)
        ECSClient client = new ECSClient("localhost", 5100);
        KVServer primaryServer = new KVServer("localhost", 5100, "localhost", 5710, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            primaryServer.putKV("testkey", "testValue");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        KVServer replicaServer = new KVServer("localhost", 5100, "localhost", 5711, 0, "None", System.getProperty("user.dir"));
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            primaryServer.close();
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
        try {
            assertEquals(replicaServer.getKV("testkey"), "testValue");
        } catch (Exception e) {

        }
    }

    @Test
    public void testStartAndStop() throws InterruptedException {
        try {
            new LogSetup("test3.log", Level.ALL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ECSClient ecsClient = new ECSClient("localhost", 5100);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Heartbeat heartbeat = new Heartbeat(ecsClient);

        assertFalse(heartbeat.isRunning());

        Thread thread = new Thread(() -> {
            heartbeat.start();
        });

        heartbeat.stop();
        assertFalse(heartbeat.isRunning());

        scheduler.shutdown();
        ecsClient.stop();
    }

    @Test
    public void testToEnsureReplicationDataSetupForLessThan3Servers() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));

            Thread.sleep(3000);
            assertEquals(server0.replicationsOfThisServer.size(), 1);
            assertEquals(server1.replicationsOfThisServer.size(), 1);
            assertEquals(server0.replicationsOfThisServer.size(), 1);
            assertEquals(server1.replicationsOfThisServer.size(), 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLeaderElection() {
        try {
            new LogSetup("test4.log", Level.ALL);
            int ecsPort = 5100;
            ECSClient ecs = new ECSClient("localhost", ecsPort);
            KVServer server0 = new KVServer("localhost", ecsPort, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server1 = new KVServer("localhost", ecsPort, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            KVServer server2 = new KVServer("localhost", ecsPort, "localhost", 38977,
                    0, "None", System.getProperty("user.dir"));
            KVServer[] servers = new KVServer[]{server0, server1, server2};
            String[] keys = new String[]{"abc", "key", "zzz"};
            Thread.sleep(3000);
            server0.putKV("abc", "val");
            server1.putKV("key", "val");
            server2.putKV("zzz", "val");

            ecs.kill();
            Thread.sleep(5000);
            KVServer leader = server0;
            String leaderKey = keys[0];

            for (int i = 1; i < servers.length; i++) {
                if (servers[i].getPriorityNum() > leader.getPriorityNum()) {
                    leader = servers[i];
                    leaderKey = keys[i];
                }
            }
            assert leader.ecsClient != null;
            // TODO: test kv transfer after LE if send sync message manually
//            for (KVServer server : servers) {
//                if (server != leader) {
//                    assertEquals(server.getKV(leaderKey), "val");
//                }
//            }
            ecsPort = leader.getPort();

            KVServer server3 = new KVServer("localhost", ecsPort, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);
            // server3 should be registered
            assert server2.checkRegisterStatus();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testServerDisconnect() {
        try {
            new LogSetup("test4.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(3000);
            KVStore kvStore = new KVStore("localhost", 42609);
            kvStore.connect();
            server0.close();
            Thread.sleep(5000);
            kvStore.updateMetadata();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}