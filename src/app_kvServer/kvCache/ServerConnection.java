package app_kvServer.kvCache;

import app_kvECS.BST;
import app_kvServer.KVServer;
import org.apache.log4j.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.zip.*;
import java.io.*;
import java.net.Socket;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ServerConnection implements Runnable{

    private static Logger logger = Logger.getRootLogger();
    private static BST ecsNodes;
    private Socket clientSocket;
    private KVServer server;

    public ServerConnection() {

    }

    public void transferData(String address, int port) {
        server.syncCacheToStorage();
        String filePath = server.getStoragePath();
        try (Socket socket = new Socket(address, port);
             FileInputStream fis = new FileInputStream(filePath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipOutputStream zos = new ZipOutputStream(socket.getOutputStream())) {
            ZipEntry zipEntry = new ZipEntry("data.txt");
            zos.putNextEntry(zipEntry);

            byte[] buffer = new byte[1024];
            int count;
            while ((count = bis.read(buffer)) > 0) {
                zos.write(buffer, 0, count);
            }

            zos.closeEntry();
            zos.finish();
        } catch (IOException e) {
            logger.error("Failed to send file data.");
        }

//        try (Socket signalSocket = new Socket(address, port);
//             PrintWriter out = new PrintWriter(signalSocket.getOutputStream(), true)) {
//            out.println("END_OF_FILE_TRANSFER");
//        }
    }

    public void receiveData() {
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

    @Override
    public void run() {

    }
}
