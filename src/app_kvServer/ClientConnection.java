package app_kvServer;

import shared.messages.KVMessage.StatusType;
import org.apache.log4j.*;

import shared.messages.ECSMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.ECSMessage.ActionType;

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
        ActionType action = msg.getAction();
        String[] range = msg.getRange();
        ECSMessage response = new ECSMessage();
        response.setAction(action);

        boolean requiresWriteLock = action == ActionType.SET_WRITE_LOCK ||
                action == ActionType.UNSET_WRITE_LOCK ||
                action == ActionType.APPEND ||
                action == ActionType.GET_DATA ||
                action == ActionType.REMOVE;

        if (requiresWriteLock) {
            String writeLockError = checkWriteLockCondition(action != ActionType.UNSET_WRITE_LOCK);
            if (writeLockError != null) {
                response.setSuccess(false);
                response.setErrorMessage(writeLockError);
                sendMessage(response);
                return;
            }
        }

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
        sendMessage(response);
        isOpen = false;
    }

    private void handleKVMessage(KVMessage msg) {
        if (msg == null) {
            isOpen = false;
            return;
        }
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
                if (!kvServer.checkRegisterStatus()) {
                    response.setStatus(StatusType.SERVER_STOPPED);
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
                break;
            default:
                logger.error("Unknown message from client: " + msg);
                break;
        }
        sendMessage(response);
    }

    private void sendMessage(Object message)  {
        if (!(message instanceof ECSMessage) && !(message instanceof KVMessage)) {
            logger.error("Unknown message type.");
        }
        try {
            output.writeObject(message);
            output.flush();
        } catch (IOException e) {
            logger.error("Failed to send message.");
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
