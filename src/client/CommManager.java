package client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.log4j.Logger;

public class CommManager {
    private Logger logger = Logger.getRootLogger();
    private Socket clientSocket;
    private BufferedWriter output;
    private BufferedReader input;

    private boolean isConnected = false;

    /**
     * Returns whether the client is connected to a server.
     * @return true if the client is connected to a server, false otherwise.
     */
    public boolean isConnected() {
        return isConnected;
    }

    /**
     * Establishes a connection to the server.
     * @param address the address of the server.
     * @param port the port of the server.
     * @throws IOException if connection could not be established.
     */
    public void connect(String address, int port)  throws IOException {
        this.clientSocket = new Socket(address, port);
        this.clientSocket.setSoTimeout(1000);
        this.output = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
        this.input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));
        isConnected = true;

        receiveMessage();
    }

    /**
     * disconnects the client from the currently connected server.
     */
    public void disconnect() {
        try {
            if (isConnected) {
                sendMessage("DISCONNECT");
                clientSocket.close();
                clientSocket = null;
                isConnected = false;
                logger.info("Connection terminated");
            }
        } catch (IOException e) {
            logger.error("Error! Unable to close connection!");
        }
    }
    
    /**
     * Sends a message using this socket.
     * @param msg the message that is to be sent.
     * @return the message that is received as a response.
     * @throws IOException
     */
    public String sendMessage(String msg) throws IOException {
        if (!isConnected) {
            logger.error("Not connected to the server");
            return null;
        }
    
        try {
            output.write(msg + "\n");
            output.flush();
            logger.info("Send message:\t '" + msg + "'");
            return receiveMessage();
        } catch (IOException e) {
            isConnected = false;
            throw e;
        }
    }

    /** 
     * Receives a message from the server.
     * @return the message received from the server.
     * @throws IOException
     */
    private String receiveMessage() throws IOException {
        if (!isConnected) {
            logger.error("Not connected to the server");
            return null;
        }

        String msg = input.readLine();
        if (msg == null) {
            isConnected = false;
            throw new IOException("Server has shut down unexpectedly");
        } else if (msg.equals("DISCONNECT")) {
            isConnected = false;
            logger.info("Connection terminated by server");
            throw new IOException("Server has terminated the connection");
        }
        return msg;
    }
}
