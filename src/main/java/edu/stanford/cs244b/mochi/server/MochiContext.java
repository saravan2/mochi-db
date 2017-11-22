package edu.stanford.cs244b.mochi.server;

import java.util.Map;

import edu.stanford.cs244b.mochi.server.datastrore.DataStore;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;

public interface MochiContext {

    public Map<Class, ServerRequestHandler<?>> getBeanRequestHandlers();

    public DataStore getBeanDataStore();

    public String getServerId();
}
