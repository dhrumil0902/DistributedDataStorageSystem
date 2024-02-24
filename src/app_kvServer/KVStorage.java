package app_kvServer;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class KVStorage {
    private final File file;
    private final Path filePath;

    private static Logger logger = Logger.getRootLogger();

    public KVStorage(String storagePath) {
        filePath = Paths.get(storagePath);
        try {
            Files.createDirectories(filePath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Could not create storage directory: " + filePath.getParent(), e);
        }
        try {
            Files.createFile(filePath);
        } catch (IOException e) {
            if (!(e instanceof FileAlreadyExistsException)) {
                throw new RuntimeException("Could not create storage file: " + filePath, e);
            }
        }
        this.file = filePath.toFile();
    }

    public synchronized void putKV(String key, String value) throws RuntimeException{
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(this.file, true));
            writer.write(key + " " + value);
            writer.newLine();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Error: Could not write to storage.", e);
        }
    }

    public synchronized void putList(List<String> data) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.file, true))) {
            for (String entry : data) {
                writer.write(entry);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error: Could not write to storage.", e);
        }
    }


    public synchronized void updateKV(String key, String value) throws RuntimeException{
        List<String> lines;
        try {
            lines = Files.readAllLines(filePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from storage file: " + filePath, e);
        }
        // look for key and update
        for (int i = 0; i < lines.size(); i++) {
            String[] kv = lines.get(i).split(" ", 2);
            if (kv.length >= 2 && kv[0].equals(key)) {
                lines.set(i, key + " " + value);
                break;
            }
        }
        // write back to storage
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to storage file: " + filePath, e);
        }
    }

    public synchronized void deleteKV(String key) throws RuntimeException{
        List<String> lines;
        try {
            lines = Files.readAllLines(filePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from storage file: " + filePath, e);
        }
        for (int i = 0; i < lines.size(); i++) {
            String[] kv = lines.get(i).split(" ", 2);
            if (kv.length >= 2 && kv[0].equals(key)) {
                lines.remove(i);
                break; // Assuming each key is unique and can only appear once
            }
        }
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to storage file: " + filePath, e);
        }
    }

    public synchronized String getKV(String key) throws RuntimeException{
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(this.file));
            String line;
            logger.info(">>>Storage");
            BufferedReader tmpReader = new BufferedReader(new FileReader(this.file));
//            Files.lines(filePath).forEach(textLine -> logger.info(textLine));
            while ((line = tmpReader.readLine()) != null) {
                logger.info(line);
            }
            logger.info("<<<");
            while ((line = reader.readLine()) != null) {
                String[] kv = line.split(" ", 2);
                if (kv.length >= 2 && kv[0].equals(key)) {
                    return kv[1];
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error: Could not read from storage.", e);
        } catch (IOException e) {
            throw new RuntimeException("Error: Storage file not found.", e);
        }
        return null;
    }

    public synchronized List<String> getAllData() throws IOException {
        return Files.readAllLines(filePath);
    }

    public synchronized boolean inStorage(String key) throws RuntimeException{
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(this.file));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] kv = line.split(" ", 2);
                if (kv.length >= 2 && kv[0].equals(key)) {
                    return true;
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not read from storage.", e);
        } catch (IOException e) {
            throw new RuntimeException("Storage file not found.", e);
        }
        return false;
    }

    public synchronized void clearStorage() throws RuntimeException{
        try {
            new FileOutputStream(this.file).close();
        } catch (IOException e) {
            throw new RuntimeException("Error: Failed to clear storage file", e);
        }
    }

    public File getFile() {
        return this.file;
    }
}
