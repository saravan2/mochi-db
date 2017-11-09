package edu.stanford.cs244b.mochi.server.messaging;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;

public class MochiMessaging implements Closeable {
    private static final int CLIENTS_MAP_SIZE = 8;
    private final ConcurrentHashMap<Server, MochiClient> clients = new ConcurrentHashMap<Server, MochiClient>(
            CLIENTS_MAP_SIZE);

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
        final MochiClient mochiClient = getClient(server);
        return mochiClient.sayHello();
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
