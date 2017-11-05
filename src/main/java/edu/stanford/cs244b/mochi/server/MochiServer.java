package edu.stanford.cs244b.mochi.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Should allow to be instantiated multiple times per JVM */
public class MochiServer {
    final static Logger LOG = LoggerFactory.getLogger(MochiServer.class);

    private final String serverId;
    private Thread mochiServerThread;
    public static final int PORT = 8081;

    public MochiServer() {
        this.serverId = Utils.getUUID();
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
                    .handler(new LoggingHandler(LogLevel.INFO)).childHandler(new MochiServerInitializer());

            b.bind(PORT).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
