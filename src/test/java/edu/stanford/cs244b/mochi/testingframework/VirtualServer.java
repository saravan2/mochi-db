package edu.stanford.cs244b.mochi.testingframework;

import org.testng.Assert;

import edu.stanford.cs244b.mochi.server.messaging.MochiServer;

public class VirtualServer {
    private final MochiServer server;

    public VirtualServer(final MochiServer s) {
        Assert.assertNotNull(s);
        this.server = s;
    }

    public MochiServer getServer() {
        return server;
    }
}
