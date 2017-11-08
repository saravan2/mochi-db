package edu.stanford.cs244b.mochi.server.messaging;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.util.StringUtils;

import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;

public class MochiMessaging implements Closeable {
    private static final int CLIENTS_MAP_SIZE = 8;
    private final ConcurrentHashMap<String, MochiClient> clients = new ConcurrentHashMap<String, MochiClient>(
            CLIENTS_MAP_SIZE);

    protected MochiClient getClient(final String serverName, final int port) {
        final String serverKey = getServerNamePort(serverName, port);
        synchronized (clients) {
            if (clients.contains(serverKey)) {
                return clients.get(serverKey);
            } else {
                MochiClient mc = new MochiClient(serverName, port);
                clients.put(serverKey, mc);
                // TODO: better place to start client
                mc.start();
                return mc;
            }
        }
    }

    protected String getServerNamePort(final String serverName, final int port) {
        Utils.assertTrue(StringUtils.isEmpty(serverName) == false);
        Utils.assertTrue(port > 0);
        return String.format("%s:%s", serverName, port);
    }

    public HelloFromServer sayHelloToServer(final String serverName, final int port) {
        final MochiClient mochiClient = getClient(serverName, port);
        return mochiClient.sayHello();
    }

    public void close() {
        synchronized (clients) {
            for (MochiClient cl : clients.values()) {
                cl.close();
            }
        }
    }

}
