package edu.stanford.cs244b.mochi.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;

public class MochiClientHandler extends SimpleChannelInboundHandler<HelloFromServer> {

    public static final String CLIENT_HELLO_MESSAGE = "Client hello";

    // Stateful properties
    private volatile Channel channel;
    private final BlockingQueue<HelloFromServer> answer = new LinkedBlockingQueue<HelloFromServer>();

    public MochiClientHandler() {
        super(false);
    }

    public HelloFromServer sayHelloToServer() {
        HelloToServer.Builder builder = HelloToServer.newBuilder();

        builder.setMsg(CLIENT_HELLO_MESSAGE);

        channel.writeAndFlush(builder.build());

        HelloFromServer helloFromServer;
        for (;;) {
            try {
                helloFromServer = answer.take();
                return helloFromServer;
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return null;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HelloFromServer times) throws Exception {
        answer.add(times);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
