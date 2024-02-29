package app_kvServer;

import app_kvECS.ECSClient;
import app_kvECS.ECSMessage;
import app_kvECS.ECSMessage.ActionType;
import shared.messages.KVMessage.StatusType;
import org.apache.log4j.*;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

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
                kvServer.setWriteLock(true);
                response.setSuccess(true);
                break;
            case UNSET_WRITE_LOCK:
                kvServer.setWriteLock(false);
                response.setSuccess(true);
                break;
            case APPEND:
                kvServer.appendDataToStorage(msg.getData());
                response.setSuccess(true);
                break;
            case GET_DATA:
                if (range == null) {
                    response.setData(kvServer.getAllData());
                } else {
                    response.setData(kvServer.getData(range[0], range[1]));
                }
                if (response.getData() != null) {
                    response.setSuccess(true);
                } else {
                    response.setSuccess(false);
                    response.setErrorMessage("Unable to append data.");
                }
                break;
            case REMOVE:
                if (kvServer.removeData(range[0], range[1])) {
                    response.setSuccess(true);
                } else {
                    response.setSuccess(false);
                    response.setErrorMessage("Unable to remove data.");
                }
                break;
            case UPDATE_METADATA:
                kvServer.updateMetadata(msg.getNodes());
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
        String key = msg.getKey();
        KVMessage response = new KVMessageImpl();
        switch (status) {
            case GET:
                response = kvServer.handleGetMessage(msg);
                break;
            case PUT:
                response = kvServer.handlePutMessage(msg);
                break;
            default:
                logger.error("Unknown message from client: " + msg);
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
