package app_kvECS;

import org.apache.log4j.Logger;
import app_kvECS.ServerConnectionListener;
import java.io.*;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private Socket clientSocket;
	private BufferedReader input;
	private BufferedWriter output;
	private ServerConnectionListener connectionListener;


	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, ServerConnectionListener connectionListener){
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.connectionListener = connectionListener;
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
			input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));

			logger.info(
					"Connection to MSRG Echo server established: "
							+ clientSocket.getLocalAddress() + " / "
							+ clientSocket.getLocalPort());
			sendMessage(connectionListener.onMessageReceived("New Node", clientSocket.getPort(), clientSocket.getRemoteSocketAddress().toString()));
			while(isOpen) {
				try {
					String latestMsg = receiveMessage();
					if (connectionListener != null) {
						sendMessage(connectionListener.onMessageReceived(latestMsg, clientSocket.getLocalPort(), clientSocket.getInetAddress().toString()));
					}
					else{
						logger.error("SERVER: Unexpected error, connectionlistener is null");
						sendMessage("ERROR");
					}

					/* connection either terminated by the client or lost due to
					 * network problems*/
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	/**
	 * Method sends a msg using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(String msg) throws IOException {
		output.write(msg + "\n");
		output.flush();
		logger.info("SEND \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '"
				+ msg +"'");
	}


	private String receiveMessage() throws IOException {
		String msg = input.readLine();

		if (msg == null) {
			isOpen = false;
			throw new IOException("Client has shut down unexpectedly");
		} else if (msg.equals("DISCONNECT")) {
			isOpen = false;
			logger.info("Connection terminated by client");
			throw new IOException("Connection terminated by client");
		}

		logger.info("RECEIVE \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '"
				+ msg + "'");
		return msg;
	}

	public void close() throws IOException{
		if (isOpen) {
			isOpen = false;
			sendMessage("DISCONNECT");
		}
	}
}
