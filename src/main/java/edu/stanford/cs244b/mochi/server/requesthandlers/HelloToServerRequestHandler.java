package edu.stanford.cs244b.mochi.server.requesthandlers;

import io.netty.channel.ChannelHandlerContext;
import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;

public class HelloToServerRequestHandler implements ServerRequestHandler<HelloToServer> {

    public static final String HELLO_RESPONSE = "Hello from Server";

    public void handle(ChannelHandlerContext ctx, ProtocolMessage protocolMessage, HelloToServer message) {
        HelloFromServer.Builder builder = HelloFromServer.newBuilder();
        builder.setClientMsg(message.getMsg());
        builder.setMsg(HELLO_RESPONSE);

        ctx.writeAndFlush(MessagesUtils.wrapIntoProtocolMessage(builder));
    }

    public Class<HelloToServer> getMessageSupported() {
        return HelloToServer.class;
    }

}
