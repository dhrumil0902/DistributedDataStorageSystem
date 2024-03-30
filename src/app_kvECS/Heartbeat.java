package app_kvECS;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.ECSNode;
import ecs.IECSNode;
import shared.BST;
import shared.messages.ECSMessage;
import shared.messages.ECSMessage.ActionType;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class Heartbeat {

    private final ECSClient ecsClient;
    private final ScheduledExecutorService scheduler;

    private static final long HEARTBEAT_INTERVAL_MS = 2000;
    private final Logger logger;
    private boolean running = false;
    private Lock lock = new ReentrantLock();

    public Heartbeat(ECSClient ecsClient) {
        this.ecsClient = ecsClient;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.logger = Logger.getRootLogger();

    }

    public void start() {
        //scheduler.scheduleAtFixedRate(ecsClient::sendHeartbeats, 4000, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        running = true;
        while (isRunning()) {
            try {
                lock.lock(); // Acquire the lock
                ecsClient.sendHeartbeats();
            } catch (Exception e) {
                System.out.println(e);
            } finally {
                lock.unlock(); // Release the lock
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                System.out.println("Interrupted while sleeping.");
                Thread.currentThread().interrupt();
            }
        }
    }


    public void stop() {
        scheduler.shutdown();
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
