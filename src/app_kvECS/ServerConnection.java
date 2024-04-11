package app_kvECS;

import app_kvECS.ECSClient;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.IECSNode;
import org.apache.log4j.*;

import shared.messages.ECSMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.utils.CommUtils;

import java.net.Socket;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class ServerConnection implements Runnable{

	private static final Logger logger = Logger.getRootLogger();
	private final Socket clientSocket;
	private final ECSClient ecsServer;
	private BufferedReader input;
	private BufferedWriter output;
	private boolean isOpen;

	public ServerConnection(Socket clientSocket, ECSClient server) {
		this.clientSocket = clientSocket;
		this.ecsServer = server;
		this.isOpen = true;
	}

	@Override
	public void run() {
		try {
			input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
			output = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8));

			while (isOpen) {
				receiveMessage();
			}
		} catch (IOException e) {
			logger.error("Error! Connection could not be established!", e);
		} finally {
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException e) {
				logger.error("Error! Unable to tear down connection!", e);
			}
		}
	}

	private void receiveMessage() {
		String msg;
		try {
			msg = input.readLine();
			if (msg == null) {
				isOpen = false;
				return;
			}

			try {
				ECSMessage obj = new ObjectMapper().readValue(msg, ECSMessage.class);
				handleECSMessage(obj);
			} catch (JsonMappingException ex) {
				logger.error("Error during message deserialization.", ex);
			} catch (IOException ex) {
				logger.error("IO error during message deserialization.", ex);
			}
		} catch (IOException e) {
//			logger.error("Error during message reception.", e);
			logger.info("Connection closed by the client.");
			isOpen = false;
		}
	}

	private void handleECSMessage(ECSMessage msg) throws IOException {
		if (msg == null) {
			isOpen = false;
			return;
		}
//		ECSMessage response = new ECSMessage();
		switch (msg.getAction()) {
			case NEW_NODE:
				if (msg.getServerInfo() != null && msg.getServerInfo().length == 2) {
					String address = msg.getServerInfo()[0];
					try {
						int port = Integer.parseInt(msg.getServerInfo()[1]);
						ecsServer.onMessageReceived("New Node", port,address);
					} catch (NumberFormatException e) {
						logger.error("Invalid port number: " + msg.getServerInfo()[1], e);
						// Optionally, set response failure or other fields here
//						response.setSuccess(false);
					}
				} else {
					logger.error("Server info (address and port) not properly set in ECSMessage.");
//					response.setSuccess(false);
				}
				break;
			case DELETE:
				if (msg.getServerInfo() != null && msg.getServerInfo().length == 2) {

					String address = msg.getServerInfo()[0];
					String port = (msg.getServerInfo()[1]);
					logger.info("Server asking to be removed from ring: " + port);
					try {
						ecsServer.removeNode(address + ":" + port, msg.getData());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				} else {
					logger.error("Server info (address and port) not properly set in ECSMessage.");
				}
				return;
			case HEARTBEAT:
				logger.info("ECS: Receive HEARTBEAT from server.");
				CommUtils.sendECSMessage(new ECSMessage(ECSMessage.ActionType.None, true, null, null, null), this.output);
				//sendMessage(new ECSMessage(ECSMessage.ActionType.None, true, null, null, null));
//				logger.info("Message SENTTTTTT");
				break;
			default:
				logger.error("Unknown action.");
		}
	}
	private void sendMessage(ECSMessage responseMessage) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = mapper.writeValueAsString(responseMessage);
		output.write(jsonString);
		output.flush();
	}

	public void close() throws IOException{
		if (isOpen) {
			isOpen = false;
//            sendMessage("DISCONNECT");
		}
	}
}
