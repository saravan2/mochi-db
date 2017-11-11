package edu.stanford.cs244b.mochi.server;

import java.util.HashMap;
import java.util.Map;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServer2RequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServerRequestHandler;

public class MochiContextImpl implements MochiContext {

    public Map<Class, ServerRequestHandler<?>> getBeanRequestHandlers() {
        final Map<Class, ServerRequestHandler<?>> handlers = new HashMap<Class, ServerRequestHandler<?>>(16);
        handlers.put(HelloToServer.class, new HelloToServerRequestHandler());
        handlers.put(HelloToServer2.class, new HelloToServer2RequestHandler());
        return handlers;
    }

}
