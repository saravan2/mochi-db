package edu.stanford.cs244b.mochi.client;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1OkFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2AnsFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;
import edu.stanford.cs244b.mochi.server.messaging.MochiMessaging;
import edu.stanford.cs244b.mochi.server.messaging.Server;

public class MochiDBClient implements Closeable {
    final static Logger LOG = LoggerFactory.getLogger(MochiDBClient.class);

    private final String mochiDBClientID = Utils.getUUIDwithPref(Utils.UUID_PREFIXES.CLIENT);
    private final MochiMessaging mochiMessaging = new MochiMessaging();
    private final Set<Server> servers = new HashSet<Server>();

    public MochiDBClient() {
    };
    
    public void addServers(final Server... servers) {
        for (final Server s : servers) {
            this.servers.add(s);
        }
    }

    public void waitForConnectionToBeEstablishedToServers() {
        for (final Server s : servers) {
            mochiMessaging.waitForConnectionToBeEstablished(s);
            LOG.debug("Connection to server {} was established", s);
        }
    }

    public void executeWriteTransaction(final Transaction transactionToExecute) {
        final Write1ToServer.Builder write1toServerBuilder = Write1ToServer.newBuilder();
        write1toServerBuilder.setTransaction(transactionToExecute);

        final List<Future<ProtocolMessage>> write1responseFutures = Utils.sendMessageToServers(write1toServerBuilder,
                servers, mochiMessaging);
        Utils.busyWaitForFutures(write1responseFutures);
        LOG.debug("Resolved write1response futures");
        final List<ProtocolMessage> write1responseProtocalMessages = Utils.getFutures(write1responseFutures);

        Write1OkFromServer writeOkFromServer1 = write1responseProtocalMessages.get(0).getWrite1OkFromServer();
        Utils.assertNotNull(writeOkFromServer1, "Expected write1ok from the server");

        Write1OkFromServer writeOkFromServer2 = write1responseProtocalMessages.get(1).getWrite1OkFromServer();
        Utils.assertNotNull(writeOkFromServer2, "Expected write1ok from the server");

        final MultiGrant server1MultiGrant = writeOkFromServer1.getMultiGrant();
        final MultiGrant server2MultiGrant = writeOkFromServer2.getMultiGrant();
        Utils.assertNotNull(server1MultiGrant, "server1MultiGrant is null");
        Utils.assertNotNull(server2MultiGrant, "server2MultiGrant is null");
        LOG.info("Got write gratns from both servers. Progressing to step 2: server1 {}. server2 {}",
                server1MultiGrant, server2MultiGrant);
        Utils.assertNotNull(server1MultiGrant.getServerId(), "server1MultiGrant.getServerId() is null");
        Utils.assertNotNull(server2MultiGrant.getServerId(), "server2MultiGrant.getServerId() is null");

        // Step 2:

        final Map<String, MultiGrant> serverGrants = new HashMap<String, MultiGrant>(2);
        serverGrants.put(server1MultiGrant.getServerId(), server1MultiGrant);
        serverGrants.put(server2MultiGrant.getServerId(), server2MultiGrant);

        final WriteCertificate.Builder wcb = WriteCertificate.newBuilder();
        wcb.putAllGrants(serverGrants);

        final Write2ToServer.Builder write2ToServerBuilder = Write2ToServer.newBuilder();
        write2ToServerBuilder.setWriteCertificate(wcb);

        final List<Future<ProtocolMessage>> write2responseFutures = Utils.sendMessageToServers(write2ToServerBuilder,
                servers, mochiMessaging);
        Utils.busyWaitForFutures(write2responseFutures);
        LOG.debug("Resolved write2response futures");
        final List<ProtocolMessage> write2responseProtocolMessages = Utils.getFutures(write2responseFutures);


        final Write2AnsFromServer write2AnsFromServer1 = write2responseProtocolMessages.get(0).getWrite2AnsFromServer();
        final Write2AnsFromServer write2AnsFromServer2 = write2responseProtocolMessages.get(1).getWrite2AnsFromServer();

        Utils.assertNotNull(write2AnsFromServer1, "write2AnsFromServer1 is null");
        Utils.assertNotNull(write2AnsFromServer2, "write2AnsFromServer2 is null");

    }

    @Override
    public void close() {
        mochiMessaging.close();
    }

}
