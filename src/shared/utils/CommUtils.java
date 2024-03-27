package shared.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import shared.messages.CoordMessage;
import shared.messages.ECSMessage;

import java.io.BufferedWriter;
import java.io.IOException;

public class CommUtils {
    private static final Logger logger = Logger.getRootLogger();
    public static void sendECSMessage(ECSMessage message, BufferedWriter output) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String jsonString = mapper.writeValueAsString(message);
            output.write(jsonString);
            output.newLine();
            output.flush();
        } catch (JsonProcessingException e) {
            System.err.println("Failed to parse ECSMessage: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Failed to send ECSMessage: " + e.getMessage());
        }
    }

    public static void sendCoordMessage(CoordMessage message, BufferedWriter output) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String jsonString = mapper.writeValueAsString(message);
            output.write(jsonString);
            output.newLine();
            output.flush();
        } catch (JsonProcessingException e) {
            // Adjust the logger to your project's configuration
            System.err.println("Failed to parse CoordMessage: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Failed to send CoordMessage: " + e.getMessage());
        }
    }
}
