package testing;

import app_kvECS.ECSClient;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest {
    private static AtomicInteger putCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        new LogSetup("logs/testing/test2.log", Level.ALL);
        ECSClient ecsClient = new ECSClient("localhost", 5201);
        runPerformanceTest( 0.8,  1000,  25);
    }

    private static void runPerformanceTest(double putRatio, int totalRequests, int numServers) throws Exception {

        for (int num = 1; num <= numServers; num++){
            KVServer kvServer = new KVServer("localhost", 5201, "localhost", 5000 + (80 * num), 0, "None", System.getProperty("user.dir"));
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        KVClient kvClient = new KVClient();
        kvClient.connect("localhost", 5080);
        double getRatio = 1 - putRatio;

        int getCount = 0;
        List<Double> randomValues = generateRandomValues(totalRequests);
        long startTime = System.currentTimeMillis();

        String filePath = "/homes/p/pate1385/ece419/ms2-group-38-good/src/testing/enronData.txt";
        List<String> words = new ArrayList<String>();

            Scanner scanner = new Scanner(new File(filePath));

            while (scanner.hasNext()) {

                String word = scanner.next();
                words.add(word);
                System.out.println(word);
            }
            scanner.close();


        for (int i = 0; i < totalRequests; i++) {
            final double randomValue = randomValues.get(i);
            final double finalPutRatio = putRatio;

                    try {
                        if (randomValue < finalPutRatio) {
                            kvClient.put(words.get(i), "some_value");
                            putCount.incrementAndGet();
                        } else {
                            kvClient.get("some_key");
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
        }
        kvClient.disconnect();
        long endTime = System.currentTimeMillis();

        double totalSeconds = (endTime - startTime) / 1000.0;
        double latency = totalSeconds / totalRequests;
        double throughput = totalRequests / totalSeconds;

        System.out.println("Latency: " + latency);
        System.out.println("GetCount: " + getCount);
        System.out.println("PutCount: " + putCount);
        System.out.println("Throughput: " + throughput);

    }

    private static List<Double> generateRandomValues(int totalRequests) {
        List<Double> randomValues = new ArrayList<>();

        for (int i = 0; i < totalRequests; i++) {
            randomValues.add(ThreadLocalRandom.current().nextDouble());
        }

        return randomValues;
    }
}