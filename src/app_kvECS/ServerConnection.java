package app_kvECS;

import app_kvECS.ECSClient;
import app_kvECS.ECSMessage;
import ecs.IECSNode;
import org.apache.log4j.*;
import shared.messages.KVMessage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.zip.*;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ServerConnection implements Runnable{

	private static final Logger logger = Logger.getRootLogger();
	private final Socket clientSocket;
	private final ECSClient ecsServer;
	private ObjectInputStream input;
	private ObjectOutputStream output;
	private boolean isOpen;

	public ServerConnection(Socket clientSocket, ECSClient server) {
		this.clientSocket = clientSocket;
		this.ecsServer = server;
		this.isOpen = true;
	}

	@Override
	public void run() {
		try {
			input = new ObjectInputStream(clientSocket.getInputStream());
			output = new ObjectOutputStream(clientSocket.getOutputStream());
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
		try {
			Object obj = input.readObject();

			if (obj instanceof ECSMessage) {
				ECSMessage message = (ECSMessage) obj;
				handleECSMessage(message);
			} else {
				logger.error("Received an unknown message type.");
			}
		} catch (EOFException e) {
			logger.info("Client has closed the connection.");
			isOpen = false;
		} catch (IOException | ClassNotFoundException e) {
			logger.error("Error during message reception and deserialization.", e);
		}
	}

	private void handleECSMessage(ECSMessage msg) throws IOException {
		if (msg == null) {
			isOpen = false;
			return;
		}
		ECSMessage response = new ECSMessage();
		switch (msg.getAction()) {
			case NEW_NODE:
				if (msg.getServerInfo() != null && msg.getServerInfo().length == 2) {
					String address = msg.getServerInfo()[0];
					try {
						int port = Integer.parseInt(msg.getServerInfo()[1]);
						response = ecsServer.onMessageReceived("New Node", port,address);
					} catch (NumberFormatException e) {
						logger.error("Invalid port number: " + msg.getServerInfo()[1], e);
						// Optionally, set response failure or other fields here
						response.setSuccess(false);
					}
				} else {
					logger.error("Server info (address and port) not properly set in ECSMessage.");
					response.setSuccess(false);
				}
				break;
			case DELETE:
				if (msg.getServerInfo() != null && msg.getServerInfo().length == 2) {

					String address = msg.getServerInfo()[0];
					String port = (msg.getServerInfo()[1]);
					logger.info("Server asking to be removed from ring: " + port);
					try {
						ecsServer.removeNode(address + port, msg.getData());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				} else {
					logger.error("Server info (address and port) not properly set in ECSMessage.");
				}
				return;
			default:
				logger.error("Unknown action.");
		}
		sendMessage(response);
	}
	private void sendMessage(ECSMessage responseMessage) throws IOException {
		output.writeObject(responseMessage);
		output.flush();
	}

	public void close() throws IOException{
		if (isOpen) {
			isOpen = false;
//            sendMessage("DISCONNECT");
		}
	}
}
