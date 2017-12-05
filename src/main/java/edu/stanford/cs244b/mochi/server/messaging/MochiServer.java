package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.Closeable;
import java.security.cert.CertificateException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.Utils;

/* Should allow to be instantiated multiple times per JVM */
public class MochiServer implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(MochiServer.class);

    private final String serverId;
    private final int serverPort;
    private volatile Thread mochiServerThread;
    private volatile boolean closed = false;
    public static final int DEFAULT_PORT = 8081;
    private static final String BIND_ON_ALL_HOST = "0.0.0.0";

    private final int executorCorePoolSize = 3;
    private final int executorMaxPoolSize = 20;
    private final long executorKeepAliveTime = 5000;
    private final BlockingQueue<Runnable> exeutorQueue = new LinkedBlockingQueue<Runnable>();
    private final ThreadPoolExecutor workerThreads;
    private final RequestHandlerDispatcher requestHandlerDispatcher;

    private final MochiContext mochiContext;

    public MochiServer(final MochiContext mochiContext) {
        this(DEFAULT_PORT, mochiContext);
    }

    public MochiServer(final int port, final MochiContext mochiContext) {
        this.serverPort = port;
        this.serverId = mochiContext.getServerId();
        workerThreads = new ThreadPoolExecutor(executorCorePoolSize, executorMaxPoolSize,
                executorKeepAliveTime, TimeUnit.MILLISECONDS, exeutorQueue);
        requestHandlerDispatcher = new RequestHandlerDispatcher(workerThreads, mochiContext);
        this.mochiContext = mochiContext;
    }

    public void start() {
        LOG.info("Starting mochi server {}", serverId);
        final boolean portAvailable = Utils.portAvailable(serverPort);
        Utils.assertTrue(portAvailable, String.format("Port %s is not available", serverPort));
        mochiServerThread = new Thread(new MochiServerListener(), getMochiServerThreadName());
        mochiServerThread.setDaemon(true);
        mochiServerThread.start();
    }

    private String getMochiServerThreadName() {
        return String.format("netty-server-%s", serverId);
    }

    public Server toServer() {
        return new Server(BIND_ON_ALL_HOST, this.serverPort);
    }

    private class MochiServerListener implements Runnable {
        public void run() {
            while (true) {
                try {
                    startServer();
                    final int retryStartServerTime = 1000;
                    Thread.sleep(retryStartServerTime);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        public void startServer() throws InterruptedException {
            if (closed) {
                LOG.warn("Server is closed. Nothing to do");
                return;
            }
            try {
                LOG.info("Mochi Server starting netty listener at {}", BIND_ON_ALL_HOST);
                startNettyListener();
            } catch (CertificateException e) {
                LOG.error("CertificateException when starting netty listener:", e);
            } catch (SSLException e) {
                LOG.error("SSLException when starting netty listener:", e);
            } catch (InterruptedException e) {
                LOG.info("InterruptedException. Exiting Mochi");
                throw e;
            } catch (java.net.BindException e) {
                LOG.info("BindException. Bad stuff.");
            } catch (Exception e) {
                LOG.error("Exception when starting netty listener:", e);
            }
        }

    }

    public void startNettyListener() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new MochiServerInitializer(requestHandlerDispatcher));

            b.bind(BIND_ON_ALL_HOST, serverPort).sync().channel().closeFuture().sync();
        } catch (Exception ex) {
            LOG.error("Exception caught when creating server:", ex);
            throw ex;
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void close() {
        closed = true;
        mochiServerThread.interrupt();
        workerThreads.shutdownNow();
    }
}
