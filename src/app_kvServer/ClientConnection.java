package app_kvServer;

import app_kvECS.ECSClient;
import app_kvECS.ECSMessage;
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

public class ClientConnection implements Runnable{

    private static final Logger logger = Logger.getRootLogger();
    private final Socket clientSocket;
    private final KVServer kvServer;
    private ObjectInputStream input;
    private ObjectOutputStream output;
    private boolean isOpen;

    public ClientConnection(Socket clientSocket, KVServer server) {
        this.clientSocket = clientSocket;
        this.kvServer = server;
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
            } else if (obj instanceof KVMessage) {
                KVMessage message = (KVMessage) obj;
                handleKVMessage(message);
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
            case SET_WRITE_LOCK:
                break;
            case APPEND:
                kvServer.appendDataToStorage(msg.getData());
                response.setAction(msg.getAction());
                response.setSuccess(true);
                break;
            case GET_ALL:
                response.setData(kvServer.getAllData());
                response.setSuccess(true);
                break;
            default:
                logger.error("Unknown identified message from kvServer: " + msg);
        }
        sendMessage(response);
    }

    private void handleKVMessage (KVMessage msg) {

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

    public void transferData(String address, int port) {
        kvServer.syncCacheToStorage();
        String filePath = kvServer.getStoragePath();

        try (Socket socket = new Socket(address, port);
             FileInputStream fis = new FileInputStream(filePath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             OutputStream out = socket.getOutputStream();
             ZipOutputStream zos = new ZipOutputStream(out);
             InputStream in = socket.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            // Send initiation message
            out.write(("BEGIN_TRANSFER\n").getBytes(StandardCharsets.UTF_8));
            out.flush();

            // Compress and send data
            ZipEntry zipEntry = new ZipEntry(new File(filePath).getName());
            zos.putNextEntry(zipEntry);
            byte[] buffer = new byte[1024];
            int count;
            while ((count = bis.read(buffer)) > 0) {
                zos.write(buffer, 0, count);
            }
            zos.closeEntry();
            zos.finish();

            // Wait for confirmation
            String msg = reader.readLine();
            if ("DATA_RECEIVED".equals(msg)) {
                logger.info(String.format("Successfully transferred data to SERVER %s:%d", address, port));
            } else {
                logger.error(String.format("Failed to transfer data to SERVER %s:%d", address, port));
            }
        } catch (IOException e) {
            logger.error(String.format("Failed to transfer data to SERVER %s:%d", address, port), e);
        }
    }


    private void receiveData() {
        try (InputStream in = clientSocket.getInputStream();
             ZipInputStream zis = new ZipInputStream(in)) {

            ZipEntry zipEntry = zis.getNextEntry();
            if (zipEntry != null) {
                String filePath = kvServer.getStoragePath();
                try (FileOutputStream fos = new FileOutputStream(filePath);
                     BufferedOutputStream bos = new BufferedOutputStream(fos)) {

                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        bos.write(buffer, 0, len);
                    }
                }
                zis.closeEntry();
            }
            System.out.println("File received and decompressed successfully.");
        } catch (IOException e) {
            System.err.println("Error processing a client connection: " + e.getMessage());
        }
    }


}
