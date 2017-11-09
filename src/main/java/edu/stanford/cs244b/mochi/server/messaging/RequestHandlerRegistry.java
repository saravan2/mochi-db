package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ThreadPoolExecutor;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServerRequestHandler;

public class RequestHandlerRegistry {

    private final ThreadPoolExecutor executor;

    public RequestHandlerRegistry(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    public void handle(final ChannelHandlerContext ctx, final ProtocolMessage protocolMessage) {
        if (protocolMessage == null) {
            throw new NullPointerException("protocolMessage should not be null");
        }
        if (protocolMessage.getHelloToServer() != null) {
            // TODO: register
            final ServerRequestHandler<HelloToServer> handler = new HelloToServerRequestHandler();
            final Runnable taskHandling = new Runnable() {

                public void run() {
                    handler.handle(ctx, protocolMessage, protocolMessage.getHelloToServer());
                }
            };
            submitTaskToHandler(taskHandling);

        }
    }

    protected void submitTaskToHandler(Runnable taskHandling) {
        executor.submit(taskHandling);
    }

}
