package app_kvServer;

import org.apache.log4j.Logger;
import shared.utils.HashUtils;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KVStorage {
    public final File file;
    public final Path filePath;

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
                // throw new RuntimeException("Could not create storage file: " + filePath, e);
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

    public synchronized List<String> getData(String minVal, String maxVal) throws IOException{
        String bottom = minVal;
        String top = maxVal;
        List<String> result = new ArrayList<>();
        logger.info("Parsing data, minval: " + minVal + ", maxVal: " + maxVal);
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2); // Split line into "key" and "value"
                if (parts.length < 2) continue; // Skip if line does not contain both key and value

                String key = parts[0];
                String hashHex = HashUtils.getHash(key);
                logger.info(key + " " + hashHex);
                if (top.compareTo(bottom) > 0) {
                    // Normal range: bottom <= hashValue <= top
                    if (hashHex.compareTo(bottom) >= 0 && hashHex.compareTo(top) <= 0) {
                        result.add(line);
                        logger.info("Added above key to result");
                    }
                } else {
                    // Corner range: hashValue <= top OR hashValue >= bottom
                    if (hashHex.compareTo(top) <= 0 || hashHex.compareTo(bottom) >= 0) {
                        logger.info("Added above key to result");
                        result.add(line);
                    }
                }
            }
        }
        return result;
    }

    public synchronized void removeAllData() {
        try {
            Files.write(filePath, Collections.emptyList(), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public synchronized void removeData(String minVal, String maxVal) throws IOException {
        BigInteger bottom = new BigInteger(minVal, 16);
        BigInteger top = new BigInteger(maxVal, 16);
        List<String> toRemove = new ArrayList<>();
        List<String> toKeep = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length < 2) continue;

                String key = parts[0];
                String hashHex = HashUtils.getHash(key);
                BigInteger hashValue = new BigInteger(hashHex, 16);

                boolean shouldRemove = (top.compareTo(bottom) > 0) ?
                        (hashValue.compareTo(bottom) >= 0 && hashValue.compareTo(top) <= 0) :
                        (hashValue.compareTo(top) <= 0 || hashValue.compareTo(bottom) >= 0);

                if (!shouldRemove) {
                    toKeep.add(line);
                } else {
                    toRemove.add(line);
                }
            }
        }

        if (!toRemove.isEmpty()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) { // false to overwrite
                for (String keepLine : toKeep) {
                    writer.write(keepLine);
                    writer.newLine();
                }
            }
        }
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
