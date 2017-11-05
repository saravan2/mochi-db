package edu.stanford.cs244b.mochi.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;

public class MochiServerHandler extends SimpleChannelInboundHandler<HelloToServer> {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HelloToServer helloServer) throws Exception {
        long currentTime = System.currentTimeMillis();

        HelloFromServer.Builder builder = HelloFromServer.newBuilder();
        builder.setClientMsg(helloServer.getMsg());
        builder.setMsg(String.format("Hello from Server: %s", Long.toString(currentTime)));
        
        ctx.write(builder.build());
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
