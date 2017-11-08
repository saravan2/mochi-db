package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat;

import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MochiClientHandler extends SimpleChannelInboundHandler<ProtocolMessage> {
    final static Logger LOG = LoggerFactory.getLogger(MochiClientHandler.class);

    public static final String CLIENT_HELLO_MESSAGE = "Client hello";

    // Stateful properties
    private volatile Channel channel;
    private ChannelHandlerContext ctx;
    private final BlockingQueue<ProtocolMessage> answer = new LinkedBlockingQueue<ProtocolMessage>();
    private BlockingQueue<Promise<ProtocolMessage>> messageList = new LinkedBlockingQueue<Promise<ProtocolMessage>>(16);
    
    public MochiClientHandler() {
        super(false);
    }

    public HelloFromServer sayHelloToServer() {
        HelloToServer.Builder builder = HelloToServer.newBuilder();
        builder.setMsg(CLIENT_HELLO_MESSAGE);

        final Promise<ProtocolMessage> p = GlobalEventExecutor.INSTANCE.newPromise();
        Future<ProtocolMessage> helloFromServerFuture = sendMessage(MessagesUtils.wrapIntoProtocolMessage(builder), p);

        ProtocolMessage pm;
        try {
            pm = helloFromServerFuture.get();
        } catch (InterruptedException e) {
            LOG.info("Interrupted");
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            LOG.error("ExecutionException: ", e);
            throw new RuntimeException("Retry");
        }
        return pm.getHelloFromServer();
    }
    
    public Future<ProtocolMessage> sendMessage(ProtocolMessage message, Promise<ProtocolMessage> prom) {
        synchronized(this){
            if(messageList == null) {
                // Connection closed
                prom.setFailure(new IllegalStateException());
            } else if(messageList.offer(prom)) { 
                // Connection open and message accepted
                ctx.writeAndFlush(message);
                LOG.debug("Wrote message {} - {}", TextFormat.shortDebugString(message), "...");
            } else { 
                // Connection open and message rejected
                prom.setFailure(new BufferOverflowException());
            }
            return prom;
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ProtocolMessage protocolMessage) throws Exception {
        answer.add(protocolMessage);
        synchronized (this) {
            if (messageList != null) {
                messageList.poll().setSuccess(protocolMessage);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        synchronized(this){
            Promise<ProtocolMessage> prom;
            Exception err = null;
            while((prom = messageList.poll()) != null) {
                Exception ex = (err != null) ? err : new IOException("Connection lost");
                prom.setFailure(ex);
            }
            messageList = null;
        }
    }
}
