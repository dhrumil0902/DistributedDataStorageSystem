package testing;

import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import app_kvECS.ECSClient;
import junit.framework.TestCase;
import shared.utils.HashUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AdditionalTest extends TestCase {

	@Test
    public void testBytesToHex() {
        byte[] byteArray = {0x0A, 0x1F, 0x2B, (byte) 0xFF}; // Example byte array
         ECSClient client = new ECSClient("t", 6);
         String temp = HashUtils.bytesToHex(byteArray);

         String c1 = "0b1";
         String c2 = "0b2";

         if (c1.compareTo(c2) > 0)
         {
             System.out.println(c1 + " is greater");
         }
         else{
             System.out.println(c2 + " is greater");
        }
        String temp1 = HashUtils.getHash("127.8.9:5500");
    }

    @Test
    public void addServer(){
        try {
            new LogSetup("test2.log", Level.ALL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ECSClient client = new ECSClient("localhost",5100);
        KVServer server = new KVServer("localhost", 5100, "localhost", 6700, 0, "None", "/homes/p/pate1385/ece419/ms2-group-38-good/src/testing/AdditionalTest.java");
        System.out.println("done");
    }

        @Test
        public void testAddNode() {
            try {
                new LogSetup("test2.log", Level.ALL);
                System.out.println("here");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ECSClient client = new ECSClient("localhost",5100);
            KVServer server = new KVServer("localhost", 5100, "localhost", 3710, 0, "None", System.getProperty("user.dir"));
            try {
                server.putKV("this", "val_test");
                server.putKV("dsdaslskdskldasklasclsalcss", "val_test");
                server.putKV("ewdfkdwloejwdflcdw", "val_test");
                server.putKV("ranyyyyyyyyyyyyyyyyyyyydom", "val_test");
                server.putKV("vfdfvfuuuuuuuuuuuuuuuuuuv", "val_test");
                server.putKV("dfvddfvkkuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu", "val_test");
                server.putKV("dvfdfvdfvdyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyf", "val_test");
                server.putKV("vdfvfffffffffffffffffffffffdfvdfv", "val_test");
                server.putKV("dfvddfvkkuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu", "val_test");
                server.putKV("dvfdfvdfvdyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyf", "val_test");
                server.putKV("vdfvfffffffffffffffffffffffdfvdfv", "val_test");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            KVServer secondserver = new KVServer("localhost", 5100, "localhost", 8710, 0, "None", System.getProperty("user.dir"));
            System.out.println("done");
        }
        @Test
        public void testGetNodeHashStartRange() {
            ECSClient client = new ECSClient("7", 7);
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
            client.addNode("temp", 56);
        }
}
