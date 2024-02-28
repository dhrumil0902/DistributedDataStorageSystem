package shared.utils;

import java.security.MessageDigest;

public class HashUtils {
    public static String getHash(String key){
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes = md.digest(key.getBytes());
            return bytesToHex(md5Bytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(b & 0xFF);
            if (hex.length() == 1) {
                hexString.append('0'); // Add leading zero if necessary
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
