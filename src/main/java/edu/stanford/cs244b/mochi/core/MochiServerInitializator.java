package edu.stanford.cs244b.mochi.core;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import edu.stanford.cs244b.mochi.server.messaging.MochiServer;

@Component
public class MochiServerInitializator {
    private final MochiServer mserver = new MochiServer();

    @PostConstruct
    public void postConstruct() {
        mserver.start();
    }
}
