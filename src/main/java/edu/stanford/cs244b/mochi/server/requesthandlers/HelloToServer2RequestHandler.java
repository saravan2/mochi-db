package edu.stanford.cs244b.mochi.server.requesthandlers;

import io.netty.channel.ChannelHandlerContext;
import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;

public class HelloToServer2RequestHandler implements ServerRequestHandler<HelloToServer2> {

    public static final String HELLO_RESPONSE = "Super Hello 2 from Server";

    public void handle(ChannelHandlerContext ctx, ProtocolMessage protocolMessage, HelloToServer2 message) {
        HelloFromServer2.Builder builder = HelloFromServer2.newBuilder();
        builder.setClientMsg(message.getMsg());
        builder.setMsg(HELLO_RESPONSE);

        ctx.writeAndFlush(MessagesUtils.wrapIntoProtocolMessage(builder));
    }

    public Class<HelloToServer2> getMessageSupported() {
        return HelloToServer2.class;
    }

}
