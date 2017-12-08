package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MochiClient implements Closeable {
    final static Logger LOG = LoggerFactory.getLogger(MochiClient.class);

    private final String server;
    private final int serverPort;
    private final String clientUUID;

    private volatile EventLoopGroup eventLoopGroup = null;
    private volatile MochiClientHandler clientHandler;
    private volatile Channel channel = null;
    private volatile ChannelFuture channelFuture = null;
    private volatile Thread connectionThread = null;

    public MochiClient(String server, int serverPort, final String clientUUID) {
        this.server = server;
        this.serverPort = serverPort;
        this.clientUUID = clientUUID;
    }

    public Future<ProtocolMessage> sendAndReceive(Object messageOrBuilder) {
        checkChannelIsOpened();
        return clientHandler.sendAndReceive(messageOrBuilder);
    }

    protected void start() {
        ThreadFactory tf = new DefaultThreadFactory(getPoolName(), Thread.MAX_PRIORITY);
        eventLoopGroup = new NioEventLoopGroup(0, tf);
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup).channel(NioSocketChannel.class).handler(new MochiClientInitializer());

        // Make a new connection.
        try {
            channelFuture = b.connect(server, serverPort);
            channel = channelFuture.sync().channel();
        } catch (InterruptedException e) {
            LOG.info("Interrupted exception");
            Thread.currentThread().interrupt();
            return;
        }
        // Get the handler instance to initiate the request.
        clientHandler = channel.pipeline().get(MochiClientHandler.class);
    }

    protected String getPoolName() {
        return String.format("mochi-client-%s-to-%s:%s", clientUUID, server, serverPort);
    }

    protected void restart() {
        close();
        start();
    }

    protected void startConnectionThreadIfNeeded() {
        synchronized (this) {
            if (connectionThread != null && connectionThread.isAlive()) {
                return;
            }
            connectionThread = new Thread(new StartChannelRunnable(), String.format("connection-thread-%s", server));
            connectionThread.setDaemon(true);
            connectionThread.start();
        }
    }

    private class StartChannelRunnable implements Runnable {

        public void run() {
            try {
                restart();
            } catch (Exception ex) {
                LOG.error("Failed to establish connection to {}:", server, ex);
            }

        }

    }

    public void waitForConnection() {
        while (true) {
            try {
                checkChannelIsOpened();
                break;
            } catch (ConnectionNotReadyException ex) {
            }
        }
    }

    protected void checkChannelIsOpened() {
        final int maxTriesToOpenChannel = 3;
        final int timeToSleepBetweenRetries = 100;
        for (int i = 0; i < maxTriesToOpenChannel; i++) {
            if (channel != null && channel.isActive()) {
                return;
            }
            startConnectionThreadIfNeeded();
            try {
                Thread.sleep(timeToSleepBetweenRetries);
            } catch (InterruptedException e) {
                LOG.info("Interrupted");
                Thread.currentThread().interrupt();
            }
        }
        if (channel != null && channel.isActive()) {
            return;
        }
        throw new ConnectionNotReadyException();
    }

    public void close() {
        if (channel != null) {
            synchronized (channel) {
                channel.close();
            }
        }
        if (eventLoopGroup != null) {
            final Future f = eventLoopGroup.shutdownGracefully();
            try {
                final boolean waitForever = false;
                if (waitForever) {
                    f.get();
                } else {
                    f.get(1000, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                LOG.debug("Sleep interrupted");
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOG.error("Failed to close");
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                LOG.error("Took too long to close");
            }
        }
    }
}
