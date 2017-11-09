package edu.stanford.cs244b.mochi.server.messaging;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol;

public class MochiServerInitializer extends ChannelInitializer<SocketChannel> {

    private final RequestHandlerRegistry requestHandlerRegistry;

    public MochiServerInitializer(RequestHandlerRegistry registry) {
        requestHandlerRegistry = registry;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(MochiProtocol.ProtocolMessage.getDefaultInstance()));

        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(new MochiServerHandler(requestHandlerRegistry));
    }
}
