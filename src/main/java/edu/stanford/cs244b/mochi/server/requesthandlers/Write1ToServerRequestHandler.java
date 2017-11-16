package edu.stanford.cs244b.mochi.server.requesthandlers;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.datastrore.DataStore;
import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1OkFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;

public class Write1ToServerRequestHandler implements ServerRequestHandler<Write1ToServer> {
    private final static Logger LOG = LoggerFactory.getLogger(Write1ToServerRequestHandler.class);
    private final MochiContext mochiContext;
    private final DataStore dataStore;

    public Write1ToServerRequestHandler(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
        dataStore = mochiContext.getBeanDataStore();
    }

    public void handle(ChannelHandlerContext ctx, ProtocolMessage protocolMessage, Write1ToServer message) {
        LOG.debug("Handling writeToServerMessage: {}", message);
        dataStore.processWrite1ToServer(message);
        // TODO: we will need here to read result and send it back over the wire

        // TODO: for now just reply OK
        LOG.debug("Sending back write reply");
        Write1OkFromServer.Builder builder = Write1OkFromServer.newBuilder();
        ctx.writeAndFlush(MessagesUtils.wrapIntoProtocolMessage(builder, protocolMessage.getMsgId()));
        LOG.debug("Wrote back response");
    }

    public Class<Write1ToServer> getMessageSupported() {
        return Write1ToServer.class;
    }

}
