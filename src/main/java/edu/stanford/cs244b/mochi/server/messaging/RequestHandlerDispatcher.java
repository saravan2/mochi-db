package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage.PayloadCase;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;

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
            wrapIntoRunnableAndSubmitTask(ctx, protocolMessage, protocolMessage.getHelloToServer(), HelloToServer.class);
        } else if (pc == PayloadCase.HELLOTOSERVER2) {
            wrapIntoRunnableAndSubmitTask(ctx, protocolMessage, protocolMessage.getHelloToServer2(),
                    HelloToServer2.class);
        } else if (pc == PayloadCase.READTOSERVER) {
            wrapIntoRunnableAndSubmitTask(ctx, protocolMessage, protocolMessage.getReadToServer(), ReadToServer.class);
        } else {
            LOG.error("Did not find message handler for message: {}", protocolMessage);
        }
    }

    private <T> void wrapIntoRunnableAndSubmitTask(final ChannelHandlerContext ctx,
            final ProtocolMessage protocolMessage, final T message, final Class<T> typeParameterClass) {
        final ServerRequestHandler<T> handler = getHandler(typeParameterClass);
        LOG.debug("Processing {} using handler {}", typeParameterClass, handler);

        final Runnable taskHandling = new Runnable() {

            public void run() {
                handler.handle(ctx, protocolMessage, message);
            }
        };
        executor.submit(taskHandling);
    }

    protected <T> ServerRequestHandler<T> getHandler(Class T) {
        final ServerRequestHandler<?> hander = handlers.get(T);
        if (hander == null) {
            final List<Class<?>> classesHandles = new ArrayList<Class<?>>();
            Enumeration<Class> classesEnum = handlers.keys();
            while (classesEnum.hasMoreElements()) {
                classesHandles.add(classesEnum.nextElement());
            }
            LOG.error("Failed to find handler to type: {} amount handlers for types {}", T, classesHandles);
        }
        return (ServerRequestHandler<T>) hander;
    }

}
