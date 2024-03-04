package app_kvServer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.log4j.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;

import shared.messages.ECSMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.ECSMessage.ActionType;
import shared.messages.KVMessage.StatusType;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.zip.*;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class ClientConnection implements Runnable {

    private static final Logger logger = Logger.getRootLogger();
    private final Socket clientSocket;
    private final KVServer kvServer;
//    private ObjectInputStream input;
//    private ObjectOutputStream output;
    private BufferedReader input;
    private BufferedWriter output;
    private boolean isOpen;

    public ClientConnection(Socket clientSocket, KVServer server) {
        this.clientSocket = clientSocket;
        this.kvServer = server;
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
            logger.info("Shut down connection.");
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
                // Attempt to deserialize the message as KVMessage first
                KVMessage message = KVMessageImpl.fromString(msg);
                logger.info("Receive KVMessage.");
                logger.info("Message: " + msg);
                handleKVMessage(message);
            } catch (IllegalArgumentException e) {
                // If deserialization fails, it might be an ECSMessage, so try that next
                logger.info("Receive ECSMessage.");
                try {
                    ECSMessage obj = new ObjectMapper().readValue(msg, ECSMessage.class);
                    handleECSMessage(obj);
                } catch (JsonMappingException ex) {
                    logger.error("Error during message deserialization.", ex);
                } catch (IOException ex) {
                    logger.error("IO error during message deserialization.", ex);
                }
            }
        } catch (IOException e) {
//            logger.error("Error during message reception.", e);
            logger.info("Connection closed by the client.");
            isOpen = false;
        }
    }

    private void handleECSMessage(ECSMessage msg) throws IOException {
        if (msg == null) {
            isOpen = false;
            return;
        }
        ActionType action = msg.getAction();
        String[] range = msg.getRange();
        ECSMessage response = new ECSMessage();
        response.setAction(action);

        boolean requiresWriteLock = action == ActionType.SET_WRITE_LOCK ||
                action == ActionType.UNSET_WRITE_LOCK ||
                action == ActionType.APPEND ||
                action == ActionType.GET_DATA ||
                action == ActionType.REMOVE;


        /*if (requiresWriteLock) {
            String writeLockError = checkWriteLockCondition(action != ActionType.UNSET_WRITE_LOCK);
            if (writeLockError != null) {
                response.setSuccess(false);
                response.setErrorMessage(writeLockError);
                sendECSMessage(response);
                return;
            }
        }*/

        switch (action) {
            case SET_WRITE_LOCK:
                logger.info("Received command to SET_WRITE_LOCK in: " + kvServer.getPort());
                if (kvServer.getWriteLock()) {
                    // write lock already set
                    response.setSuccess(false);
                    response.setErrorMessage("Write lock already set.");
                    break;
                }
                logger.info("SET_WRITE_LOCK successfull in: " + kvServer.getPort());
                kvServer.setWriteLock(true);
                response.setSuccess(true);
                break;
            case UNSET_WRITE_LOCK:
                logger.info("Received command to UNSET_WRITE_LOCK in: " + kvServer.getPort());
                if (!kvServer.getWriteLock()) {
                    // write lock not set
                    response.setSuccess(false);
                    response.setErrorMessage("Write lock not set.");
                    break;
                }
                logger.info("UNSET_WRITE_LOCK successfully in: " + kvServer.getPort());
                kvServer.setWriteLock(false);
                response.setSuccess(true);
                break;
            case APPEND:
                logger.info("Received command to append data in: " + kvServer.getPort());
                if (!kvServer.getWriteLock()) {
                    // write lock not set
                    response.setSuccess(false);
                    response.setErrorMessage("Write lock not set.");
                    break;
                }
                logger.info("Append data successful: " + kvServer.getPort());
                kvServer.appendDataToStorage(msg.getData());
                response.setSuccess(true);
                break;
            case GET_DATA:
                logger.info("Received command to GET_DATA in: " + kvServer.getPort());
                if (!kvServer.getWriteLock()) {
                    // write lock not set
                    response.setSuccess(false);
                    response.setErrorMessage("Write lock not set.");
                    break;
                }
                if (range == null) {
                    response.setData(kvServer.getAllData());
                } else {
                    logger.info("Setting data in response of port: " + kvServer.getPort());
                    response.setData(kvServer.getData(range[0], range[1]));
                }
                if (response.getData() != null) {
                    logger.info("Command GET_DATA successfully returning in: " + kvServer.getPort() + "'sending data of size: " + response.getData().size());
                    response.setSuccess(true);
                } else {
                    response.setSuccess(false);
                    response.setErrorMessage("Unable to append data.");
                }
                break;
            case REMOVE:
                logger.info("Received command to REMOVE in: " + kvServer.getPort());
                logger.info("Range: " + range[0] + " : " + range[1]);
                if (!kvServer.getWriteLock()) {
                    logger.error("NOT able to REMOVE data in: " + kvServer.getPort());
                    // write lock not set
                    response.setSuccess(false);
                    response.setErrorMessage("Write lock not set.");
                    break;
                }
                if (kvServer.removeData(range[0], range[1])) {
                    logger.info("Successfully able to REMOVE data in: " + kvServer.getPort());
                    response.setSuccess(true);
                } else {
                    response.setSuccess(false);
                    response.setErrorMessage("Unable to remove data.");
                }
                break;
            case UPDATE_METADATA:
                logger.info("Received command to UPDATE_METADATA in: " + kvServer.getPort());
                kvServer.updateMetadata(msg.getNodes());
                logger.info("Successfully updated metadata in: " + kvServer.getPort());
                response.setSuccess(true);
                break;
            case DELETE:
                kvServer.close();
                break;
            default:
                logger.error("Unknown message from ECS: " + msg);
        }
        sendECSMessage(response);
        isOpen = false;
    }

    private void handleKVMessage(KVMessage msg) {
        StatusType status = msg.getStatus();
        KVMessage response = new KVMessageImpl();
        switch (status) {
            case GET:
                if (!kvServer.checkRegisterStatus()) {
                    response.setStatus(StatusType.SERVER_STOPPED);
                    break;
                }
                response = kvServer.handleGetMessage(msg);
                break;
            case PUT:
                logger.info("In put.");
                if (!kvServer.checkRegisterStatus()) {
                    response.setStatus(StatusType.SERVER_STOPPED);
                    logger.info("server not register");
                    break;
                }
                response = kvServer.handlePutMessage(msg);
                break;
            case KEYRANGE:
                if (!kvServer.checkRegisterStatus()) {
                    response.setStatus(StatusType.SERVER_STOPPED);
                    break;
                }
                response = kvServer.handleKeyRangeMessage(msg);
                break;
            case DISCONNECT:
                isOpen = false;
                logger.info("Connection terminated by client");
                response.setStatus(KVMessage.StatusType.DISCONNECT);
                break;
            default:
                isOpen = false;
                logger.error("Unknown message from client: " + msg);
                return;
        }
        logger.info("Sending message: " + response.toString());
        sendKVMessage(response.toString());
    }

    private void sendECSMessage(ECSMessage message)  {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String jsonString = mapper.writeValueAsString(message);
            output.write(jsonString);
            output.newLine();
            output.flush();
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse ECSMessage.");
        } catch (IOException e) {
            logger.error("Failed to send ECSMessage.");
        }
    }

    public void sendKVMessage(String msg) {
        try {
            output.write(msg + "\n");
            output.flush();
        } catch (IOException e) {
            logger.error("Failed to send KVMessage.");
        }
    }


    public void close() throws IOException {
        if (isOpen) {
            isOpen = false;
//            sendMessage("DISCONNECT");
        }
    }

    private String checkWriteLockCondition(boolean requireLock) {
        if (requireLock && !kvServer.getWriteLock()) {
            return "Write lock not set.";
        } else if (!requireLock && kvServer.getWriteLock()) {
            return "Write lock already set.";
        }
        return null; // indicates no error
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
