package app_kvServer.kvCache;

import app_kvECS.BST;
import app_kvServer.KVServer;
import org.apache.log4j.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.zip.*;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ServerConnection implements Runnable{

    private static final Logger logger = Logger.getRootLogger();
    private static BST ecsNodes;
    private final Socket clientSocket;
    private final KVServer server;
    private BufferedReader input;
    private BufferedWriter output;
    private boolean isOpen;

    public ServerConnection(Socket clientSocket, KVServer server) {
        this.clientSocket = clientSocket;
        this.server = server;
    }

    @Override
    public void run() {
        try {
            input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
            output = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8));
            while (isOpen) {
                handleMessage();
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

    private void handleMessage() throws IOException {
        String msg = input.readLine();
        if (msg == null) {
            isOpen = false;
        }
        switch (msg) {
            case "BEGIN_TRANSFER":
                receiveData();
                sendMessage("DATA_RECEIVED");
            default:
                logger.error("Unknown identified message from server: " + msg);
        }
    }

    private void sendMessage(String msg) throws IOException {
        output.write(msg + "\n");
        output.flush();
    }

    public void transferData(String address, int port) {
        server.syncCacheToStorage();
        String filePath = server.getStoragePath();

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
                String filePath = server.getStoragePath();
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
