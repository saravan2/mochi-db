package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol;

public class MochiServerInitializer extends ChannelInitializer<SocketChannel> {
    private final static Logger LOG = LoggerFactory.getLogger(MochiServerInitializer.class);

    private final RequestHandlerDispatcher requestHandlerDispatcher;

    public MochiServerInitializer(RequestHandlerDispatcher dispatcher) {
        requestHandlerDispatcher = dispatcher;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(MochiProtocol.ProtocolMessage.getDefaultInstance()));

        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(new MochiServerHandler(requestHandlerDispatcher));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.info("Exception caught");
        super.exceptionCaught(ctx, cause);
    }
}
