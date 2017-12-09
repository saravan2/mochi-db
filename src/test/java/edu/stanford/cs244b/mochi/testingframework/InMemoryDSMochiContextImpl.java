package edu.stanford.cs244b.mochi.testingframework;

import edu.stanford.cs244b.mochi.server.ClusterConfiguration;
import edu.stanford.cs244b.mochi.server.MochiContextImpl;

public class InMemoryDSMochiContextImpl extends MochiContextImpl {
    private final ClusterConfiguration clusterConfiguration;

    public InMemoryDSMochiContextImpl(final ClusterConfiguration clusterConfiguration, final String serverId) {
        this.clusterConfiguration = new ClusterConfiguration(clusterConfiguration);
        setServerId(serverId);
    }

    @Override
    public ClusterConfiguration getClusterConfiguration() {
        return clusterConfiguration;
    }

}
