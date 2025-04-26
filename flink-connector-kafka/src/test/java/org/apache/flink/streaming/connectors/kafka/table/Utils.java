package org.apache.flink.streaming.connectors.kafka.table;

import java.util.Random;

/** Common testing utilities. */
public class Utils {

    // Define the characters to be used in the random string
    private static final String CHARACTERS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    public static final Random RANDOM = new Random();

    public static String generateRandomString() {
        int minLength = 10;
        int maxLength = 50;

        int length = RANDOM.nextInt(maxLength - minLength + 1) + minLength;

        final StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }

        return sb.toString();
    }
}
