package edu.stanford.cs244b.mochi.server;

import java.util.UUID;

public class Utils {

    public static class UUID_PREFIXES {
        public static final String SERVER = "server";
        public static final String CLIENT = "client";
    }

    public static String getUUID() {
        return UUID.randomUUID().toString();
    }

    public static String getUUIDwithPref(final String pref) {
        return String.format("%s-%s", pref, UUID.randomUUID().toString());
    }

    public static void assertTrue(boolean cond) {
        if (!cond) {
            throw new IllegalStateException("Assertion failed");
        }
    }

    public static void assertNotNull(final Object o, final String msg) {
        if (o == null) {
            throw new IllegalStateException(msg);
        }
    }
}
