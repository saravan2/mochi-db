package edu.stanford.cs244b.mochi.core;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.MochiContextImpl;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;

@Component
public class MochiServerInitializator {
    private final MochiContext mochiContext = new MochiContextImpl();
    private final MochiServer mserver;

    public MochiServerInitializator() {
        mserver = new MochiServer(mochiContext);
    }

    @PostConstruct
    public void postConstruct() {
        mserver.start();
    }
}
