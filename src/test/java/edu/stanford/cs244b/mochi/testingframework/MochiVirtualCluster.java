package edu.stanford.cs244b.mochi.testingframework;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;
import edu.stanford.cs244b.mochi.server.messaging.Server;

/* 
 Represents virual cluster which contains of different clients and servers, 
 all run on the same JVM. The primarily goal of that - ease of testing
 */
public class MochiVirtualCluster implements Closeable {
    private final int initialPort = 8001;
    private volatile int nextPort = initialPort;

    private final List<VirtualServer> servers;
    
    public MochiVirtualCluster(int initialNumberOfServers, int bftFautlyReplicas) {
        Assert.assertTrue(initialNumberOfServers > 0);
        Assert.assertTrue(bftFautlyReplicas > 0);
        checkNumberOfFaultyCorrect(initialNumberOfServers, bftFautlyReplicas);
        servers = new ArrayList<VirtualServer>(initialNumberOfServers * 2);
        for (int i = 0 ; i < initialNumberOfServers; i++) {
            addMochiServer();
        }
    }

    private void checkNumberOfFaultyCorrect(int servers, int bftFautlyReplicas) {
        int minNumberOfServers = 3 * bftFautlyReplicas + 1;
        if (servers < minNumberOfServers) {
            throw new IllegalStateException("Too little servers to support specified BFT");
        }
    }

    private void addMochiServer() {
        final InMemoryDSMochiContextImpl mochiContext = new InMemoryDSMochiContextImpl();
        final MochiServer ms = newMochiServer(nextPort, mochiContext);
        nextPort += 1;
        servers.add(new VirtualServer(ms, mochiContext));
    }

    private MochiServer newMochiServer(final int serverPort, final MochiContext mochiContext) {
        return new MochiServer(serverPort, mochiContext);
    }

    public void startAllServers() {
        for (final VirtualServer vs : servers) {
            final MochiServer s = vs.getServer();
            s.start();
        }
    }

    public List<Server> getAllServers() {
        final List<Server> mServers = new ArrayList<Server>(servers.size());
        for (final VirtualServer vs : servers) {
            final MochiServer s = vs.getServer();
            mServers.add(s.toServer());
        }
        return mServers;
    }

    @Override
    public void close() {
        for (final VirtualServer vs : servers) {
            final MochiServer s = vs.getServer();
            s.close();
        }
    }
}
