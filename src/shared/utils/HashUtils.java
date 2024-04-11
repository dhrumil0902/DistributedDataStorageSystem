package shared.utils;

import java.math.BigInteger;
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

    public static boolean evaluateKeyHash(String key, String minVal, String maxVal) {
        if (minVal.compareTo(maxVal) == 0) {
            return true;
        }

        BigInteger bottom = new BigInteger(minVal, 16);
        BigInteger top = new BigInteger(maxVal, 16);
        String hashHex = getHash(key);
        BigInteger hashValue = new BigInteger(hashHex, 16);
        if (top.compareTo(bottom) > 0) {
            // Normal range: bottom <= hashValue <= top
            return hashValue.compareTo(bottom) >= 0 && hashValue.compareTo(top) <= 0;
        } else if (top.compareTo(bottom) < 0) {
            // Corner range: hashValue <= top OR hashValue >= bottom
            return hashValue.compareTo(top) <= 0 || hashValue.compareTo(bottom) >= 0;
        }
        return false;
    }

    public static void main(String[] args) {
        String key = "zzz";
        System.out.println(getHash(key));
    }
}
