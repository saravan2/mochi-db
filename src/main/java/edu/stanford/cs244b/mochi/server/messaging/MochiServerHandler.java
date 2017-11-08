package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MochiServerHandler extends SimpleChannelInboundHandler<ProtocolMessage> {
    final static Logger LOG = LoggerFactory.getLogger(MochiServerHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ProtocolMessage protocolMessage) throws Exception {
        LOG.debug("Got message {}", protocolMessage);
        long currentTime = System.currentTimeMillis();

        HelloFromServer.Builder builder = HelloFromServer.newBuilder();
        builder.setClientMsg(protocolMessage.getHelloToServer().getMsg());
        builder.setMsg(String.format("Hello from Server: %s", Long.toString(currentTime)));
        
        ctx.writeAndFlush(MessagesUtils.wrapIntoProtocolMessage(builder));
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
