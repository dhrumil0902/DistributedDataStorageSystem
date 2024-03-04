package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest {
    private static AtomicInteger putCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        new LogSetup("logs/testing/test2.log", Level.ALL);
        ECSClient ecsClient = new ECSClient("localhost", 5201);
        runPerformanceTest( 0.8,  100,  10);
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
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Double> randomValues = generateRandomValues(totalRequests);
        double getRatio = 1 - putRatio;

        int getCount = 0;

        long startTime = System.currentTimeMillis();
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < totalRequests; i++) {
            final double randomValue = randomValues.get(i);
            final double finalPutRatio = putRatio;

            Runnable task = new Runnable() {
                @Override
                public void run() {
                    KVStore kvStore = new KVStore("localhost", 50000);
                    try {
                        kvStore.connect();

                        if (randomValue < finalPutRatio) {
                            kvStore.put("some_key" + randomValue, "some_value");
                            putCount.incrementAndGet();
                        } else {
                            kvStore.get("some_key");
                        }

                        kvStore.disconnect();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            futures.add(CompletableFuture.runAsync(task, executorService));
        }

        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allOf.join();
        long endTime = System.currentTimeMillis();

        double totalSeconds = (endTime - startTime) / 1000.0;
        double latency = totalSeconds / totalRequests;
        double throughput = totalRequests / totalSeconds;

        System.out.println("Latency: " + latency);
        System.out.println("GetCount: " + getCount);
        System.out.println("PutCount: " + putCount);
        System.out.println("Throughput: " + throughput);

        executorService.shutdown();
    }

    private static List<Double> generateRandomValues(int totalRequests) {
        List<Double> randomValues = new ArrayList<>();

        for (int i = 0; i < totalRequests; i++) {
            randomValues.add(ThreadLocalRandom.current().nextDouble());
        }

        return randomValues;
    }
}