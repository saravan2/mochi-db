package edu.stanford.cs244b.mochi.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
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
}
