package edu.stanford.cs244b.mochi.server.messaging;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MochiMessaging implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(MochiMessaging.class);

    private static final int CLIENTS_MAP_SIZE = 8;
    private final ConcurrentHashMap<Server, MochiClient> clients = new ConcurrentHashMap<Server, MochiClient>(
            CLIENTS_MAP_SIZE);

    public static final String CLIENT_HELLO_MESSAGE = "Client hello";

    protected MochiClient getClient(final Server server) {
        synchronized (clients) {
            if (clients.contains(server)) {
                return clients.get(server);
            } else {
                MochiClient mc = new MochiClient(server.getServerName(), server.getPort());
                clients.put(server, mc);
                mc.startConnectionThreadIfNeeded();
                return mc;
            }
        }
    }

    public HelloFromServer sayHelloToServer(final Server server) {
        // TODO: rewrite that message
        HelloToServer.Builder builder = HelloToServer.newBuilder();
        builder.setMsg(MochiMessaging.CLIENT_HELLO_MESSAGE);

        Future<ProtocolMessage> helloFromServerFuture = sendAndReceive(server, builder.build());

        ProtocolMessage pm;
        try {
            pm = helloFromServerFuture.get();
        } catch (InterruptedException e) {
            LOG.info("Interrupted");
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            LOG.error("ExecutionException: ", e);
            throw new RuntimeException("Retry");
        }
        return pm.getHelloFromServer();
    }

    public HelloFromServer2 sayHelloToServer2(final Server server) {
        // TODO: rewrite that message
        HelloToServer2.Builder builder = HelloToServer2.newBuilder();
        builder.setMsg(MochiMessaging.CLIENT_HELLO_MESSAGE);

        Future<ProtocolMessage> helloFromServer2Future = sendAndReceive(server, builder.build());

        ProtocolMessage pm;
        try {
            pm = helloFromServer2Future.get();
        } catch (InterruptedException e) {
            LOG.info("Interrupted");
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            LOG.error("ExecutionException: ", e);
            throw new RuntimeException("Retry");
        }
        return pm.getHelloFromServer2();
    }

    public Future<ProtocolMessage> sendAndReceive(final Server server, final Object messageOrBuilder) {
        final MochiClient mochiClient = getClient(server);
        return mochiClient.sendAndReceive(messageOrBuilder);
    }

    public void waitForConnectionToBeEstablished(final Server server) {
        final MochiClient mochiClient = getClient(server);
        mochiClient.waitForConnection();
    }

    public void close() {
        synchronized (clients) {
            for (MochiClient cl : clients.values()) {
                cl.close();
            }
        }
    }

}
