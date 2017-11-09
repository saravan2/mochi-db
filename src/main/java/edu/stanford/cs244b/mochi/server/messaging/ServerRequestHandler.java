package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public interface ServerRequestHandler<T> {

    public Class<T> getMessageSupported();

    public void handle(ChannelHandlerContext ctx, ProtocolMessage protocolMessage, T message);
}
