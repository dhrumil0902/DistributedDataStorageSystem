package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.Assert;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import shared.Heartbeat;
import shared.messages.CoordMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.utils.HashUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class M4TestCases extends TestCase {
    @Test
    public void testHighestPriorityNumWinsElection() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);

            List<KVServer> servers = new ArrayList<>();
            servers.add(server0);
            servers.add(server1);
            servers.add(server2);

            KVServer serverWithHighestPriority = servers.stream()
                    .max(Comparator.comparingInt(KVServer::getPriorityNum))
                    .orElse(null);

            ecs.kill();
            Thread.sleep(4000);
            Assert.assertNotNull(serverWithHighestPriority.ecsClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testOnlyOneLeaderChosen() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 46683,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);

            List<KVServer> servers = new ArrayList<>();
            servers.add(server0);
            servers.add(server1);
            servers.add(server2);

            ecs.kill();
            Thread.sleep(4000);

            int countLeaders = 0;

            if (server0.ecsClient != null) {
                countLeaders += 1;
            }
            if (server1.ecsClient != null) {
                countLeaders += 1;
            }
            if (server2.ecsClient != null) {
                countLeaders += 1;
            }

            Assert.assertEquals(countLeaders, 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNewECSClientCanAddServer() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5130);
            KVServer server0 = new KVServer("localhost", 5130, "localhost", 42613,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            ecs.kill();
            Thread.sleep(4000);
            KVServer server1 = new KVServer("localhost", 5130, "localhost", 46677,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);

            Assert.assertEquals(server1.getMetadata().size(), 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testServerDisconnect() {
        try {
            new LogSetup("test4.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5110);
            KVServer server0 = new KVServer("localhost", 5110, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            KVServer server1 = new KVServer("localhost", 5110, "localhost", 42157,
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

    @Test
    public void testRangeUpdateForServersOnNewLeader() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5120);
            KVServer server0 = new KVServer("localhost", 5120, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5120, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server2 = new KVServer("localhost", 5120, "localhost", 42158,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            ecs.kill();
            Thread.sleep(4000);

            Assert.assertEquals(server0.getMetadata().get(server0.getHashValue()).getNodeHashRange()[1], server1.getMetadata().get(server0.getHashValue()).getNodeHashRange()[1]);
            Assert.assertEquals(server1.getMetadata().get(server1.getHashValue()).getNodeHashRange()[0], server0.getMetadata().get(server0.getHashValue()).getNodeHashRange()[1]);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testReplicationDataSetForServersOnNewLeader() {
        try {
            new LogSetup("test3.log", Level.ALL);
            ECSClient ecs = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5100, "localhost", 42157,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server2 = new KVServer("localhost", 5100, "localhost", 42158,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server3 = new KVServer("localhost", 5100, "localhost", 42159,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            ecs.kill();
            Thread.sleep(4000);

            assertEquals(server0.replicationsOfThisServer.size(), 2);
            assertEquals(server1.replicationsOfThisServer.size(), 2);
            assertEquals(server2.replicationsOfThisServer.size(), 2);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testClientServerDisconnect() {
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

    @Test
    public void testHeartBeatSendingFromServerToESCClient() {
        try {
            new LogSetup("test10.log", Level.ALL);
            ECSClient ecsClient = new ECSClient("localhost", 5180);
            KVServer server0 = new KVServer("localhost", 5180, "localhost", 42809,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(5000);

            Assert.assertTrue(logContainsMessage("test10.log", "Send heartbeat to"));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean logContainsMessage(String logFilePath, String message) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains(message)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    public void testPersistentDataStorageOnLastServerBecomingLeader() {
        try {
            new LogSetup("test10.log", Level.ALL);
            ECSClient ecsClient = new ECSClient("localhost", 5100);
            KVServer server0 = new KVServer("localhost", 5100, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            server0.putKV("testkey", "testkey");
            ecsClient.kill();
            Thread.sleep(5000);
            Assert.assertNotNull(server0.getKV("testkey"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNewLeaderDeletesAndTransfersItsOwnDataWhenThereAreOtherServersLeft() {
        try {
            new LogSetup("test10.log", Level.ALL);
            ECSClient ecsClient = new ECSClient("localhost", 5110);
            KVServer server0 = new KVServer("localhost", 5110, "localhost", 42609,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            KVServer server1 = new KVServer("localhost", 5110, "localhost", 42610,
                    0, "None", System.getProperty("user.dir"));
            Thread.sleep(1000);
            server1.putKV("testkey", "testkey");
            ecsClient.kill();
            Thread.sleep(5000);
            Assert.assertNull(server1.getKV("testkey"));
            Assert.assertNotNull(server0.getKV("testkey"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}