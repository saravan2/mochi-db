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
    public static final String CLIENT_HELLO2_MESSAGE = "Client hello 2";

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
        final HelloToServer.Builder builder = HelloToServer.newBuilder();
        builder.setMsg(MochiMessaging.CLIENT_HELLO_MESSAGE);

        final ProtocolMessage pm = makeRequestAndWaitForPMFuture(server, builder.build());

        return pm.getHelloFromServer();
    }

    public Future<ProtocolMessage> sayHelloToServerAsync(final Server server) {
        final HelloToServer.Builder builder = HelloToServer.newBuilder();
        builder.setMsg(MochiMessaging.CLIENT_HELLO_MESSAGE);

        final Future<ProtocolMessage> responseFromServerFuture = sendAndReceive(server, builder);

        return responseFromServerFuture;
    }

    public HelloFromServer2 sayHelloToServer2(final Server server) {
        final HelloToServer2.Builder builder = HelloToServer2.newBuilder();
        builder.setMsg(MochiMessaging.CLIENT_HELLO2_MESSAGE);

        final ProtocolMessage pm = makeRequestAndWaitForPMFuture(server, builder.build());

        return pm.getHelloFromServer2();
    }

    // TODO: create two new methods to send read request - regular way and async

    protected ProtocolMessage makeRequestAndWaitForPMFuture(final Server server, final Object messageOrBuilder) {
        final Future<ProtocolMessage> responseFromServerFuture = sendAndReceive(server, messageOrBuilder);
        ProtocolMessage pm;
        try {
            pm = responseFromServerFuture.get();
        } catch (InterruptedException e) {
            LOG.info("Interrupted");
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            LOG.error("ExecutionException: ", e);
            throw new RuntimeException("Retry");
        }
        LOG.debug("Got message from server {}", pm);
        return pm;
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
