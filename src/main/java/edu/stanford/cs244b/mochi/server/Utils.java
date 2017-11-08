package edu.stanford.cs244b.mochi.server;

import java.util.UUID;

public class Utils {

    public static String getUUID() {
        return UUID.randomUUID().toString();
    }

    public static void assertTrue(boolean cond) {
        if (!cond) {
            throw new IllegalStateException("Assertion failed");
        }
    }
}
