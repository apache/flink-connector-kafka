package org.apache.flink.connector.kafka.sink;

public class ClientIdFactory {
    private static final String CLIENT_ID_DELIMITER = "-";

    public static String buildClientId(
            String clientIdPrefix,
            int subtaskId
    ) {
        return clientIdPrefix
                + CLIENT_ID_DELIMITER
                + subtaskId;
    }
}
