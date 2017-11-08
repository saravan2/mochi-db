package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;

public class MochiClient implements Closeable {
    final static Logger LOG = LoggerFactory.getLogger(MochiClient.class);

    private final String server;
    private final int serverPort;

    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private volatile MochiClientHandler clientHandler;
    private volatile Channel channel;

    public MochiClient(String server, int serverPort) {
        this.server = server;
        this.serverPort = serverPort;
    }

    protected void start() {

        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup).channel(NioSocketChannel.class).handler(new MochiClientInitializer());

        // Make a new connection.
        try {
            channel = b.connect(server, serverPort).sync().channel();
        } catch (InterruptedException e) {
            LOG.info("Interrupted exception");
            Thread.currentThread().interrupt();
            return;
        }
        // Get the handler instance to initiate the request.
        clientHandler = channel.pipeline().get(MochiClientHandler.class);
    }

    public HelloFromServer sayHello() {
        HelloFromServer hfs = clientHandler.sayHelloToServer();
        return hfs;
    }

    public void close() {
        if (channel != null) {
            synchronized (channel) {
                channel.close();
            }
        }
        eventLoopGroup.shutdownGracefully();
    }
}
