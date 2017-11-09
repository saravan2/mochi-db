package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat;

import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MochiClientHandler extends SimpleChannelInboundHandler<ProtocolMessage> {
    final static Logger LOG = LoggerFactory.getLogger(MochiClientHandler.class);

    // Stateful properties
    private volatile Channel channel;
    private ChannelHandlerContext ctx;
    private final BlockingQueue<ProtocolMessage> answer = new LinkedBlockingQueue<ProtocolMessage>();
    private BlockingQueue<Promise<ProtocolMessage>> messageList = new LinkedBlockingQueue<Promise<ProtocolMessage>>(16);
    
    public MochiClientHandler() {
        super(false);
    }


    public Future<ProtocolMessage> sendAndReceive(Object messageOrBuilder) {
        final Promise<ProtocolMessage> p = GlobalEventExecutor.INSTANCE.newPromise();
        Future<ProtocolMessage> helloFromServerFuture = sendMessage(
                MessagesUtils.wrapIntoProtocolMessage(messageOrBuilder), p);
        return helloFromServerFuture;
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
        // TODO: that assumes that message order is correct
        answer.add(protocolMessage);
        synchronized (this) {
            if (messageList != null) {
                messageList.poll().setSuccess(protocolMessage);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Caught exception at MochiClientHander:", cause);
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
