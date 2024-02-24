package app_kvECS;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class UniqueRandomNumberGenerator {
    private static Set<Integer> generatedNumbers = new HashSet<>();
    private static Random random = new Random();

    public static int generateUniqueFourDigitNumber() {
        int randomNumber;
        do {
            randomNumber = random.nextInt(9000) + 1000;
        } while (!generatedNumbers.add(randomNumber)); // Keep generating until a unique number is found
        return randomNumber;
    }

    public static void main(String[] args) {
        // Test the generateUniqueFourDigitNumber method
        for (int i = 0; i < 10; i++) {
            int uniqueNumber = generateUniqueFourDigitNumber();
            System.out.println("Unique 4-digit number " + (i + 1) + ": " + uniqueNumber);
        }
    }
}

