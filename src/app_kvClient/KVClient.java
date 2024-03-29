package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import shared.messages.KVMessage;

public class KVClient implements IKVClient {
    private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "M1Client> ";
	private boolean stop = false;
    
    private KVStore kvStore;
	private BufferedReader stdin;
	private String serverAddress;
	private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        if (kvStore != null) {
            if (kvStore.getAddress().equals(hostname) && kvStore.getPort() == port && kvStore.isConnected()) {
                printMsg("Already connected to " + hostname + ":" + port);
                return;
            } else {
                kvStore.disconnect();
            }
        }

        kvStore = new KVStore(hostname, port);
        kvStore.connect();
        printMsg("Connected to " + kvStore.getAddress() + ":" + kvStore.getPort());
    }

    public void connect(String hostname, int port) {
        try {
            newConnection(hostname, port);
        } catch (Exception e) {
            printError(e.getMessage());
        }
    }

    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

    public void disconnect() {
        if (kvStore != null && kvStore.isConnected()) {
            kvStore.disconnect();
            printMsg("Disconnected from " + kvStore.getAddress() + ":" + kvStore.getPort());
        } else {
            printError("Attempting to disconnect while not connected to a server ... doing nothing");
        }
    }

    public void put(String key, String value) {
        try {
            KVMessage response = kvStore.put(key, value);
            switch (response.getStatus()) {
                case PUT_SUCCESS:
                    printMsg("Successfully put <" + key + ", " + value + ">");
                    break;
                case PUT_UPDATE:
                    printMsg("Successfully updated <" + key + ", " + value + ">");
                    break;
                case DELETE_SUCCESS:
                    printMsg("Successfully deleted <" + key + ">");
                    break;
                case DELETE_ERROR:
                    printError("Error deleting <" + key + ">");
                    break;
                case PUT_ERROR:
                    printError("Error putting <" + key + ", " + value + ">");
                    break;
                case SERVER_WRITE_LOCK:
                    printError(String.format("Failed to put <%s, %s>: Server write lock is set.", key, value));
                default:
                    printError("Unknown status: " + response.getStatus());
            }
        } catch (Exception e) {
            logger.error(e);
            printError("Error putting <" + key + ", " + value + ">: " + e.getMessage());
        }
    }

    public void get(String key) {
        try {
            KVMessage response = kvStore.get(key);
            switch (response.getStatus()) {
                case GET_SUCCESS:
                    printMsg("Value for <" + key + ">: " + response.getValue());
                    break;
                case GET_ERROR:
                    printError("Key <" + key + "> not found");
                    break;
                default:
                    printError("Unknown status: " + response.getStatus());
            }
        } catch (Exception e) {
            logger.error(e);
            printError("Error getting <" + key + ">: " + e.getMessage());
        }
        
    }

    public void shutdown() {
        stop = true;
        if (kvStore != null) {
            kvStore.disconnect();
        }
        printMsg("Application terminated");
    }

    public void logLevel(String levelString) {
        try {
            LogSetup.setLevel(levelString);
            printMsg("Log level changed to level " + levelString);
        } catch (Exception e) {
            printError("Invalid log level, please enter one of: " + LogSetup.getPossibleLogLevels());
        }
    }


    public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

    private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

        if (tokens.length == 0) {
            printError("No command entered");
            printHelp();
        }

        try {
            switch (tokens[0]) {
                case "connect":
                    if (tokens.length == 3) {
                        serverAddress = tokens[1];
                        serverPort = Integer.parseInt(tokens[2]);
                        connect(serverAddress, serverPort);
                    } else {
                        printError("Invalid number of parameters, usage: connect <host> <port>");
                    }
                    break;
                case "disconnect":
                    disconnect();
                    break;
                case "put":
                    if (tokens.length >= 3) {
                        put(tokens[1], cmdLine.substring(tokens[0].length() + tokens[1].length() + 2));
                    } else {
                        printError("Invalid number of parameters, usage: put <key> <value>");
                    }
                    break;
                case "get":
                    if (tokens.length == 2) {
                        get(tokens[1]);
                    } else {
                        printError("Invalid number of parameters, usage: get <key>");
                    }
                    break;
                case "logLevel":
                    if (tokens.length == 2) {
                        logLevel(tokens[1]);
                    } else {
                        printError("Invalid number of parameters, usage: logLevel <level>");
                    }
                    break;
                case "help":
                    printHelp();
                    break;
                case "quit":
                    shutdown();
                    break;
                default:
                    printError("Unknown command");
                    printHelp();
            }
        } catch (Exception e) {
            printError(e.getMessage());
        }
	}

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
		logger.error(error);
	}

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("M1CLIENT HELP:\n");
        sb.append(PROMPT).append(":::::::::::::::::::::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "connect <host> <port>", "Establishes connection to a server"));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "disconnect", "Disconnect from the server"));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "put <key> <value>", "Insert/Update a key-value pair"));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "get <key>", "Retrieve the value of a key"));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "logLevel <level>", "Set the log level:"));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "", LogSetup.getPossibleLogLevels()));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "help", "Show this help message"));
        sb.append(PROMPT).append(String.format("%-25s%-50s\n", "quit", "Exit the program"));
        System.out.println(sb.toString());
    }

    private void printMsg(String msg) {
        System.out.println(PROMPT + msg);
    }

    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.ALL);
			final KVClient client = new KVClient();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    client.shutdown();
                }
            });
            client.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
