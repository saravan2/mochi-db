package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServer2RequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServerRequestHandler;

public class RequestHandlerDispatcher {
    private final static Logger LOG = LoggerFactory.getLogger(RequestHandlerDispatcher.class);

    private final ThreadPoolExecutor executor;
    
    private final ConcurrentHashMap<Class, ServerRequestHandler<?>> handlers = new ConcurrentHashMap<Class, ServerRequestHandler<?>>(
            16);

    public RequestHandlerDispatcher(ThreadPoolExecutor executor) {
        this.executor = executor;
        addHandlers();
    }
    
    private void addHandlers() {
        handlers.put(HelloToServer.class, new HelloToServerRequestHandler());
        handlers.put(HelloToServer2.class, new HelloToServer2RequestHandler());
    }

    public void handle(final ChannelHandlerContext ctx, final ProtocolMessage protocolMessage) {
        if (protocolMessage == null) {
            throw new NullPointerException("protocolMessage should not be null");
        }
        if (protocolMessage.getHelloToServer() != null) {
            final ServerRequestHandler<HelloToServer> handler = getHandler(HelloToServer.class);
            LOG.debug("Processing helloToServerMessage using handler {}", handler);
            final Runnable taskHandling = new Runnable() {

                public void run() {
                    handler.handle(ctx, protocolMessage, protocolMessage.getHelloToServer());
                }
            };
            submitTaskToHandler(taskHandling);

        } else {
            LOG.error("Did not find message handler for message: {}", protocolMessage);
        }
    }

    protected <T> ServerRequestHandler<T> getHandler(Class T) {
        final ServerRequestHandler<?> hander = handlers.get(T);
        if (hander == null) {
            LOG.error("Failed to find handler to type: {}", T);
        }
        return (ServerRequestHandler<T>) hander;
    }

    protected void submitTaskToHandler(Runnable taskHandling) {
        executor.submit(taskHandling);
    }

}
