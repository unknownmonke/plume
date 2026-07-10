package org.plume.common;

public class Constants {

    public static final String ACKS = "all";
    private static final String DEAD_LETTER_QUEUE_SUFFIX = "_dlq";

    public static String getDlqTopic(String dlqTopic, String originalTopic) {
        return dlqTopic != null
            ? dlqTopic : originalTopic + DEAD_LETTER_QUEUE_SUFFIX;
    }
}
