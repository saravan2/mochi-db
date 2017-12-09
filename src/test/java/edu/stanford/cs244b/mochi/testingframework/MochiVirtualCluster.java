package edu.stanford.cs244b.mochi.testingframework;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;

import edu.stanford.cs244b.mochi.client.MochiDBClient;
import edu.stanford.cs244b.mochi.server.ClusterConfiguration;
import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.MochiContextImpl;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;
import edu.stanford.cs244b.mochi.server.messaging.Server;

/* 
 Represents virual cluster which contains of different clients and servers, 
 all run on the same JVM. The primarily goal of that - ease of testing
 */
public class MochiVirtualCluster implements Closeable {
    private final int initialPort = 8001;
    private volatile int nextPort = initialPort;

    private final HashMap<String, VirtualServer> servers;
    private final HashMap<Integer, String> tokenDivision;
    private volatile int bftReplicationFactor;
    
    // We have the same cluster configuration for all tests
    final ClusterConfiguration clusterConfiguration = new ClusterConfiguration();

    public MochiVirtualCluster() {
        this(5, 4);
    }

    public MochiVirtualCluster(int initialNumberOfServers, int bftReplicationFactor) {
        Assert.assertTrue(initialNumberOfServers >= 4);
        Assert.assertTrue(bftReplicationFactor >= 4);
        Assert.assertTrue(bftReplicationFactor <= initialNumberOfServers);
        
        servers = new HashMap<String, VirtualServer>(initialNumberOfServers * 2);
        tokenDivision = new HashMap<Integer, String>(ClusterConfiguration.SHARD_TOKENS);
        this.bftReplicationFactor = bftReplicationFactor;

        for (int i = 0; i < initialNumberOfServers; i++) {
            servers.put(MochiContextImpl.generateNewServerId(), null);
        }
        giveTokensToServers(initialNumberOfServers);

        setInitialMochiClusterConfiguration();

        for (final String serverId : new HashSet<String>(servers.keySet())) {
            addMochiServer(serverId);
        }

    }
    
    public MochiDBClient getMochiDBClient() {
        final MochiDBClient mochiDBclient = new MochiDBClient(clusterConfiguration);
        mochiDBclient.addServers(this.getAllServers());
        return mochiDBclient;
    }

    private void giveTokensToServers(int initialNumberOfServers) {
        final String[] serverIds = servers.keySet().toArray(new String[0]);
        Arrays.sort(serverIds); // To have determinism during tests
        for (int i = 0; i < ClusterConfiguration.SHARD_TOKENS; i++) {
            final String serverId = serverIds[ i % serverIds.length];
            tokenDivision.put(i, serverId);
        }
    }

    private void setInitialMochiClusterConfiguration() {
        // Dividing tokens between servers
        final ClusterConfiguration cc = this.clusterConfiguration;
        final Properties props = new Properties();
        final Map<String, String> serverProps = cc.putTokensAroundRingProps(new ArrayList<String>(servers.keySet()));
        props.putAll(serverProps);
        props.put(ClusterConfiguration.PROPERTY_BFT_REPLICATION, Integer.toString(this.bftReplicationFactor));
        cc.loadInitialConfigurationFromProperties(props);
    }

    private void checkNumberOfFaultyCorrect(int servers, int bftFautlyReplicas) {
        int minNumberOfServers = 3 * bftFautlyReplicas + 1;
        if (servers < minNumberOfServers) {
            throw new IllegalStateException("Too little servers to support specified BFT");
        }
    }

    private void addMochiServer(final String serverId) {
        final InMemoryDSMochiContextImpl mochiContext = new InMemoryDSMochiContextImpl(clusterConfiguration, serverId);
        final MochiServer ms = newMochiServer(nextPort, mochiContext);
        nextPort += 1;
        final VirtualServer vs = new VirtualServer(ms, mochiContext);
        Assert.assertEquals(mochiContext.getServerId(), serverId);
        servers.put(serverId, vs);
    }

    private MochiServer newMochiServer(final int serverPort, final MochiContext mochiContext) {
        return new MochiServer(serverPort, mochiContext);
    }

    public void startAllServers() {
        for (final VirtualServer vs : servers.values()) {
            final MochiServer s = vs.getServer();
            s.start();
        }
    }

    public List<Server> getAllServers() {
        final List<Server> mServers = new ArrayList<Server>(servers.size());
        for (final VirtualServer vs : servers.values()) {
            final MochiServer s = vs.getServer();
            mServers.add(s.toServer());
        }
        return mServers;
    }

    @Override
    public void close() {
        for (final VirtualServer vs : servers.values()) {
            final MochiServer s = vs.getServer();
            s.close();
        }
    }
}
