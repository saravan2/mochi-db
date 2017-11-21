package edu.stanford.cs244b.mochi.server.requesthandlers;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.datastrore.DataStore;
import edu.stanford.cs244b.mochi.server.messages.MessagesUtils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2ToServer;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;

public class Write2ToServerRequestHandler implements ServerRequestHandler<Write2ToServer> {
    private final static Logger LOG = LoggerFactory.getLogger(Write2ToServerRequestHandler.class);
    private final MochiContext mochiContext;
    private final DataStore dataStore;

    public Write2ToServerRequestHandler(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
        dataStore = mochiContext.getBeanDataStore();
    }

    public void handle(ChannelHandlerContext ctx, ProtocolMessage protocolMessage, Write2ToServer message) {
        LOG.debug("Handling writeToServer2Message: {}", message);
        final Object write2response = dataStore.processWrite2ToServer(message);
        LOG.debug("Sending back write2 reply: {}", write2response);
        ctx.writeAndFlush(MessagesUtils.wrapIntoProtocolMessage(write2response, protocolMessage.getMsgId()));
        LOG.debug("Wrote back response");
    }

    public Class<Write2ToServer> getMessageSupported() {
        return Write2ToServer.class;
    }

}
