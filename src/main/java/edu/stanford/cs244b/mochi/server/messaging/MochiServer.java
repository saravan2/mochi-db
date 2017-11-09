package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.Closeable;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.Utils;

/* Should allow to be instantiated multiple times per JVM */
public class MochiServer implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(MochiServer.class);

    private final String serverId;
    private final int serverPort;
    private Thread mochiServerThread;
    public static final int DEFAULT_PORT = 8081;
    private static final String BIND_LOCALHOST = "localhost";

    private final int executorCorePoolSize = 3;
    private final int executorMaxPoolSize = 20;
    private final long executorKeepAliveTime = 5000;
    private final BlockingQueue<Runnable> exeutorQueue = new LinkedBlockingQueue<Runnable>();
    private final ThreadPoolExecutor workerThreads;
    private final RequestHandlerDispatcher requestHandlerDispatcher;

    public MochiServer() {
        this(DEFAULT_PORT);
    }

    public MochiServer(final int port) {
        this.serverPort = port;
        this.serverId = Utils.getUUID();
        workerThreads = new ThreadPoolExecutor(executorCorePoolSize, executorMaxPoolSize,
                executorKeepAliveTime, TimeUnit.MILLISECONDS, exeutorQueue);
        requestHandlerDispatcher = new RequestHandlerDispatcher(workerThreads);
    }

    public void start() {
        LOG.info("Starting mochi server {}", serverId);
        mochiServerThread = new Thread(new MochiServerListener(), getMochiServerThreadName());
        mochiServerThread.setDaemon(true);
        mochiServerThread.start();
    }

    private String getMochiServerThreadName() {
        return String.format("netty-server-%s", serverId);
    }

    public Server toServer() {
        return new Server(BIND_LOCALHOST, this.serverPort);
    }

    private class MochiServerListener implements Runnable {
        public void run() {
            try {
                try {
                    startNettyListener();
                } catch (CertificateException e) {
                    LOG.error("CertificateException when starting netty listener:", e);
                } catch (SSLException e) {
                    LOG.error("SSLException when starting netty listener:", e);
                }
            } catch (InterruptedException e) {
                LOG.info("InterruptedException. Exiting Mochi");
                return;
            }
        }

    }

    public void startNettyListener() throws InterruptedException, CertificateException, SSLException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new MochiServerInitializer(requestHandlerDispatcher));

            b.bind(BIND_LOCALHOST, serverPort).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void close() throws IOException {
        workerThreads.shutdownNow();
    }
}
