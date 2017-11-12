package edu.stanford.cs244b.mochi.server.requesthandlers;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.datastrore.DataStore;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messaging.ServerRequestHandler;

public class ReadToServerRequestHandler implements ServerRequestHandler<ReadToServer> {
    private final static Logger LOG = LoggerFactory.getLogger(ReadToServerRequestHandler.class);
    private final MochiContext mochiContext;
    private final DataStore dataStore;

    public ReadToServerRequestHandler(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
        dataStore = mochiContext.getBeanDataStore();
    }

    public void handle(ChannelHandlerContext ctx, ProtocolMessage protocolMessage, ReadToServer message) {
        final Transaction transactionToExecute = message.getTransaction();
        LOG.debug("Executing read transaction {}", transactionToExecute);
        dataStore.executeTransaction(transactionToExecute, true);
        // ctx.writeAndFlush(MessagesUtils.wrapIntoProtocolMessage(builder,
        // protocolMessage.getMsgId()));
    }

    public Class<ReadToServer> getMessageSupported() {
        return ReadToServer.class;
    }

}
