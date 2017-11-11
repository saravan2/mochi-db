package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MochiServerHandler extends SimpleChannelInboundHandler<ProtocolMessage> {
    private final static Logger LOG = LoggerFactory.getLogger(MochiServerHandler.class);

    private final RequestHandlerDispatcher requestHandlerDispatchet;

    public MochiServerHandler(RequestHandlerDispatcher requestHandlerDispatcher) {
        this.requestHandlerDispatchet = requestHandlerDispatcher;
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, ProtocolMessage protocolMessage) throws Exception {
        LOG.debug("Got message {}", protocolMessage);
        requestHandlerDispatchet.handle(ctx, protocolMessage);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Caught exception while processing messages");
        ctx.close();
    }

}
