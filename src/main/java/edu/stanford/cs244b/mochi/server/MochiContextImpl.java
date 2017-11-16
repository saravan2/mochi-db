package edu.stanford.cs244b.mochi.server;

import java.util.HashMap;
import java.util.Map;

import edu.stanford.cs244b.mochi.server.datastrore.DataStore;
import edu.stanford.cs244b.mochi.server.datastrore.InMemoryDataStore;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServer2RequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServerRequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.ReadToServerRequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.Write1ToServerRequestHandler;

public class MochiContextImpl implements MochiContext {
    private volatile Map<Class, ServerRequestHandler<?>> handlers = null;
    private volatile DataStore dataStore = null;

    public synchronized Map<Class, ServerRequestHandler<?>> getBeanRequestHandlers() {
        if (handlers != null) {
            return handlers;
        }
        handlers = new HashMap<Class, ServerRequestHandler<?>>(16);
        addHandler(new HelloToServerRequestHandler(), handlers);
        addHandler(new HelloToServer2RequestHandler(), handlers);
        addHandler(new ReadToServerRequestHandler(this), handlers);
        addHandler(new Write1ToServerRequestHandler(this), handlers);

        return handlers;
    }

    private void addHandler(ServerRequestHandler<?> handler, Map<Class, ServerRequestHandler<?>> handlers) {
        handlers.put(handler.getMessageSupported(), handler);
    }

    public synchronized DataStore getBeanDataStore() {
        if (dataStore != null) {
            return dataStore;
        }
        dataStore = new InMemoryDataStore();
        return dataStore;
    }

}
