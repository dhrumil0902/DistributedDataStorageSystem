package app_kvECS;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    public Heartbeat(ECSClient ecsClient) {
        this.ecsClient = ecsClient;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.logger = Logger.getRootLogger();

    }

    public void start() {
        scheduler.scheduleAtFixedRate(ecsClient::sendHeartbeats, 4000, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }


    public void stop() {
        scheduler.shutdown();
    }
}
