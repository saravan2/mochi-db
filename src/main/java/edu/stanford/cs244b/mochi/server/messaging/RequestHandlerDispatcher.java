package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage.PayloadCase;

public class RequestHandlerDispatcher {
    private final static Logger LOG = LoggerFactory.getLogger(RequestHandlerDispatcher.class);

    private final ThreadPoolExecutor executor;
    private final MochiContext mochiContext;
    private final ConcurrentHashMap<Class, ServerRequestHandler<?>> handlers = new ConcurrentHashMap<Class, ServerRequestHandler<?>>(
            16);

    public RequestHandlerDispatcher(ThreadPoolExecutor executor, final MochiContext mochiContext) {
        this.executor = executor;
        this.mochiContext = mochiContext;
        handlers.putAll(mochiContext.getBeanRequestHandlers());
    }
    
    public void handle(final ChannelHandlerContext ctx, final ProtocolMessage protocolMessage) {
        if (protocolMessage == null) {
            throw new NullPointerException("protocolMessage should not be null");
        }
        final PayloadCase pc = protocolMessage.getPayloadCase();
        LOG.debug("Decising on payload message with PayloadCase {}", pc);
        // TODO: make dynamic
        if (pc == PayloadCase.HELLOTOSERVER) {
            final ServerRequestHandler<HelloToServer> handler = getHandler(HelloToServer.class);
            LOG.debug("Processing helloToServerMessage using handler {}", handler);
            final Runnable taskHandling = new Runnable() {

                public void run() {
                    handler.handle(ctx, protocolMessage, protocolMessage.getHelloToServer());
                }
            };
            submitTaskToHandler(taskHandling);

        } else if (pc == PayloadCase.HELLOTOSERVER2) {
            final ServerRequestHandler<HelloToServer2> handler = getHandler(HelloToServer2.class);
            LOG.debug("Processing helloToServerMessage2 using handler {}", handler);
            final Runnable taskHandling = new Runnable() {

                public void run() {
                    handler.handle(ctx, protocolMessage, protocolMessage.getHelloToServer2());
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
