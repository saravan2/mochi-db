package edu.stanford.cs244b.mochi.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;

public class MochiClient {
    final static Logger LOG = LoggerFactory.getLogger(MochiServer.class);

    public HelloFromServer sayHello() throws InterruptedException {

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new MochiClientInitializer());

            // Make a new connection.
            Channel ch = b.connect("localhost", MochiServer.PORT).sync().channel();

            // Get the handler instance to initiate the request.
            MochiClientHandler handler = ch.pipeline().get(MochiClientHandler.class);

            HelloFromServer hfs = handler.sayHelloToServer();

            // Close the connection.
            ch.close();

            return hfs;

        } finally {
            group.shutdownGracefully();
        }
    }
}
