package edu.stanford.cs244b.mochi.core;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.MochiContextImpl;
import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;

@Component
public class MochiServerInitializator {
    public static final String CURRENT_SERVER_KEY = "clusterCurrentServer";

    private final MochiContext mochiContext;
    private final MochiServer mserver;

    public MochiServerInitializator() {
        final String currentServerId = System.getProperty(CURRENT_SERVER_KEY);
        Utils.assertNotNull(currentServerId,
                String.format("Missing property %s. Current server unknown", CURRENT_SERVER_KEY));
        mochiContext = new MochiContextImpl(currentServerId);
        mserver = new MochiServer(mochiContext);
    }

    @PostConstruct
    public void postConstruct() {
        mserver.start();
    }
}
