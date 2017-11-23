package edu.stanford.cs244b.mochi.testingframework;

import org.testng.Assert;

import edu.stanford.cs244b.mochi.server.messaging.MochiServer;

public class VirtualServer {
    private final MochiServer server;
    private final InMemoryDSMochiContextImpl context;

    public VirtualServer(final MochiServer s, final InMemoryDSMochiContextImpl context) {
        Assert.assertNotNull(s);
        this.server = s;
        this.context = context;
    }

    public MochiServer getServer() {
        return server;
    }

    public InMemoryDSMochiContextImpl getContext() {
        return context;
    }
}
