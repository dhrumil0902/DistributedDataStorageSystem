package app_kvServer;

import app_kvECS.ECSClient;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class HeartbeatServer {

    private final ScheduledExecutorService scheduler;

    private static final long HEARTBEAT_INTERVAL_MS = 2000;
    private final Logger logger;
    private final KVServer kvServer;
    public boolean running = false;
    private Lock lock = new ReentrantLock();

    public HeartbeatServer(KVServer server) {
        this.kvServer = server;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.logger = Logger.getRootLogger();

    }

    public void start() {
        //scheduler.scheduleAtFixedRate(ecsClient::sendHeartbeats, 4000, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        running = true;
        while (isRunning()) {
            try {
                kvServer.sendHeartbeats();
            } catch (Exception e) {
                System.out.println(e);
            }

            try {
                Thread.sleep(4110);
            } catch (InterruptedException e) {
                System.out.println("Interrupted while sleeping.");
                Thread.currentThread().interrupt();
            }
        }
    }


    public void stop() {
        running = false;
        scheduler.shutdown();
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
