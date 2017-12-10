package edu.stanford.cs244b.mochi.server;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;
import edu.stanford.cs244b.mochi.server.messaging.MochiMessaging;
import edu.stanford.cs244b.mochi.server.messaging.Server;

public class Utils {
    final static Logger LOG = LoggerFactory.getLogger(Utils.class);

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

    public static void assertTrue(boolean cond, final String msg) {
        if (!cond) {
            throw new IllegalStateException(msg);
        }
    }

    public static void assertNotNull(final Object o) {
        assertNotNull(o, "Object is null");
    }

    public static void assertNotNull(final Object o, final String msg) {
        if (o == null) {
            throw new IllegalStateException(msg);
        }
    }

    public static <T> void busyWaitForFutures(final List<Future<T>> futures) {
        busyWaitForFutures(futures, futures.size());
    }

    public static <T> void busyWaitForFutures(final List<Future<T>> futures, int countToWait) {
        if (countToWait > futures.size()) {
            throw new IllegalArgumentException("Cannot wait for more futures than in the list");
        }
        final Set<Future<?>> futuresResolved = new HashSet<Future<?>>();
        while (true) {
            for (Future<?> future : futures) {
                if (futuresResolved.contains(future)) {
                    continue;
                }
                if (future.isCancelled() || future.isDone()) {
                    futuresResolved.add(future);
                }
            }
            if (futuresResolved.size() >= countToWait) {
                break;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                LOG.info("Caught InterruptedException: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public static <T> List<T> getFutures(final List<Future<T>> futures) {
        final List<T> resolvedFutures = new ArrayList<T>(futures.size());
        for (final Future<T> future : futures) {
            T resolvedFuture;
            try {
                resolvedFuture = future.get();
            } catch (InterruptedException e) {
                LOG.error("InterruptedException:", e);
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                LOG.error("ExecutionException:", e);
                throw new RuntimeException(e);
            }
            resolvedFutures.add(resolvedFuture);
        }
        return resolvedFutures;
    }

    public static List<Future<ProtocolMessage>> sendMessageToServers(final Object messageOrBuilder,
            final Set<Server> servers, final MochiMessaging mm) {
        final List<Future<ProtocolMessage>> responseFutures = new ArrayList<Future<ProtocolMessage>>(
                servers.size());
        for (final Server s : servers) {
            final Future<ProtocolMessage> responseFuture = mm.sendAndReceive(s, messageOrBuilder);
            Utils.assertNotNull(responseFuture, "responseFuture should not be null");
            responseFutures.add(responseFuture);
        }
        return responseFutures;
    }

    public static OperationResult getOperationResult(final TransactionResult transaction1result, int position) {
        final List<OperationResult> operationList = transaction1result.getOperationsList();
        Utils.assertNotNull(operationList, "operationList is null");
        if (operationList.size() <= position) {
            throw new IllegalArgumentException("position is outOfBound");
        }
        final OperationResult or = operationList.get(position);
        return or;
    }

    public static String sha512(final byte[] data) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get sha-512 implementation", e);
        }
        byte[] bytes = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    public static String objectSHA512(final Serializable object) {
        byte[] data = SerializationUtils.serialize(object);
        return sha512(data);
    }

    public static boolean portAvailable(int port) {
        final int MIN_PORT_NUMBER = 0;
        final int MAX_PORT_NUMBER = 65535;
        if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }
}
