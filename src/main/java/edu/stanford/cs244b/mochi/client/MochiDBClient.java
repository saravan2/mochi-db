package edu.stanford.cs244b.mochi.client;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jcabi.aspects.Loggable;

import edu.stanford.cs244b.mochi.server.ClusterConfiguration;
import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Grant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResultStatus;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage.PayloadCase;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1OkFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1RefusedFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2AnsFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;
import edu.stanford.cs244b.mochi.server.messaging.MochiMessaging;
import edu.stanford.cs244b.mochi.server.messaging.Server;

public class MochiDBClient implements Closeable {
    final static Logger LOG = LoggerFactory.getLogger(MochiDBClient.class);

    private final AtomicLong operationNumberCounter = new AtomicLong(1);
    private final String mochiDBClientID = Utils.getUUIDwithPref(Utils.UUID_PREFIXES.CLIENT);
    private final MochiMessaging mochiMessaging = new MochiMessaging(mochiDBClientID);

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private volatile JmxReporter metricsJMXreporter;

    private final Timer metricsReadTransactionsTimer = metricRegistry.timer(getMetricName("read-transactions"));
    private final Timer metricsReadTransactionsStep1WaitTimer = metricRegistry
            .timer(getMetricName("read-transactions-step1-future-wait"));
    private final Timer metricsWriteTransactionsTimer = metricRegistry.timer(getMetricName("write-transactions"));

    private final ClusterConfiguration clusterConfiguration;

    public MochiDBClient(final ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
        exposeMetricsOverJMX();
    };
    
    protected void exposeMetricsOverJMX() {
        metricsJMXreporter = JmxReporter.forRegistry(metricRegistry).build();
        metricsJMXreporter.start();
    }

    public String getClientID() {
        return mochiDBClientID;
    }

    public void waitForConnectionToBeEstablishedToServers() {
        for (final Server s : clusterConfiguration.getAllServers()) {
            mochiMessaging.waitForConnectionToBeEstablished(s);
            LOG.debug("Connection to server {} was established", s);
        }
    }

    protected String getMetricName(final String name) {
        return String.format("%s-%s-%s", this.getClass().getSimpleName(), this.mochiDBClientID, name);
    }

    public long getNextOperationNumber() {
        return operationNumberCounter.getAndIncrement();
    }
    
    protected void validateThatAllResponsesAreOk(final TransactionResult tr) {
        Utils.assertNotNull(tr);
        final List<OperationResult> operationResult = tr.getOperationsList();
        for (OperationResult result : operationResult) {
            if (result.getStatus() == OperationResultStatus.WRONG_SHARD) {
                throw new IllegalStateException(
                        "Response should not contain wrong shard. Client should have merged the results");
            }
        }
    }

    @Loggable()
    public TransactionResult executeReadTransaction(final Transaction transactionToExecute) {
        final Timer.Context context = this.metricsReadTransactionsTimer.time();
        try {
            final TransactionResult tr = executeReadTransactionBL(transactionToExecute);
            validateThatAllResponsesAreOk(tr);
            return tr;
        } finally {
            context.stop();
        }
    }

    private TransactionResult executeReadTransactionBL(final Transaction transactionToExecute) {
        final ReadToServer.Builder rbuilder = ReadToServer.newBuilder();
        rbuilder.setClientId(Utils.getUUID());
        rbuilder.setNonce(Utils.getUUID());
        rbuilder.setTransaction(transactionToExecute);

        Set<Server> relevantServers = new HashSet<Server>();
        final List<Operation> transactionOps = transactionToExecute.getOperationsList();
        for (final Operation op : transactionOps) {
            Set<Server> serverSet = clusterConfiguration.getServerSetStoringObject(op.getOperand1());
            relevantServers.addAll(serverSet);
        }
        
        final List<Future<ProtocolMessage>> readResponseFutures = Utils.sendMessageToServers(rbuilder, relevantServers,
                mochiMessaging);

        final Timer.Context context = this.metricsReadTransactionsStep1WaitTimer.time();
        try {
            Utils.busyWaitForFutures(readResponseFutures);
        } finally {
            context.stop();
        }
        LOG.debug("Resolved readResponse futures");
        
        final List<ProtocolMessage> readResponseProtocalMessages = Utils.getFutures(readResponseFutures);
        
        final List<ReadFromServer> readFromServers = new ArrayList<ReadFromServer>(relevantServers.size());
        for (final ProtocolMessage pm : readResponseProtocalMessages) {
            final ReadFromServer readFromServer = pm.getReadFromServer();
            Utils.assertNotNull(readFromServer, "readFromServer is null");
            readFromServers.add(readFromServer);
        }
        // To get transaction result, we can select any readFromServers
        // TODO: build TransactionResult by getting different responses
        int[] consistentTRCount = new int[transactionOps.size()];
        List<OperationResult> coalescedResult = new ArrayList<OperationResult>(transactionOps.size());
        
        for (int index = 0; index < transactionOps.size(); index++) {
            coalescedResult.add(null);
            consistentTRCount[index] = 0;
        }
        
        for (ReadFromServer response : readFromServers) {
            TransactionResult tr = response.getResult();
            final List<OperationResult> operations = tr.getOperationsList();
            if (operations.size() != transactionOps.size()) {
                throw new InconsistentReadException();
            }
            for (int index = 0; index < transactionOps.size(); index++) {
                OperationResult or = operations.get(index);
                if (or.getStatus() != OperationResultStatus.WRONG_SHARD) {
                    consistentTRCount[index] = consistentTRCount[index] + 1;
                    coalescedResult.set(index, or);
                }
            }
        }
        
        for (int index = 0; index < transactionOps.size(); index++) {
            if (consistentTRCount[index] < clusterConfiguration.getServerMajority()) {
                throw new InconsistentReadException();
            }
        }
                
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(coalescedResult);
        final TransactionResult transactionResult = transactionResultBuilder.build();
        return transactionResult;
    }

    @Loggable()
    public TransactionResult executeWriteTransaction(final Transaction transactionToExecute) {
        final Timer.Context context = this.metricsWriteTransactionsTimer.time();
        try {
            final TransactionResult tr = executeWriteTransactionBL(transactionToExecute);
            validateThatAllResponsesAreOk(tr);
            return tr;
        } finally {
            context.stop();
        }
    }
    
    private boolean isUniformTimeStampInMultiGrants(Map<String, MultiGrant> multiGrantsFromAllServers, Transaction transaction) {      
        Utils.assertNotNull(multiGrantsFromAllServers, "MultiGrants map should not be null");
        HashMap<String, Pair<Long, Grant>> coalescedTxnGrantMap = new HashMap<String, Pair<Long, Grant>>();
        for (final MultiGrant multiGrant : multiGrantsFromAllServers.values()) {
            final Map<String, Grant> allGrants = multiGrant.getGrantsMap();
            Utils.assertNotNull(allGrants, "Grants map should not be null");
            final List<Operation> transactionOps = transaction.getOperationsList();
                for (final Operation op : transactionOps) {
                    final Grant grantForCurrentKey = allGrants.get(op.getOperand1());
                    if (grantForCurrentKey == null) {
                        continue;
                    }
                    long timestampFromGrant = grantForCurrentKey.getTimestamp();
                    if (coalescedTxnGrantMap.containsKey(op.getOperand1())) {
                        if (coalescedTxnGrantMap.get(op.getOperand1()).getValue0() != timestampFromGrant) {
                            return false;
                        }   
                    } else {
                        Pair<Long, Grant> entry = new Pair<Long, Grant>(timestampFromGrant, grantForCurrentKey);
                        coalescedTxnGrantMap.put(op.getOperand1(), entry);
                    }
                }
        }
        return true;
    }
    
    private MultiGrant removeWrongShardGrantFromMultiGrant(MultiGrant mg) {
        Map<String, Grant> grants = mg.getGrantsMap();
        Set<String> interestedKeys = grants.keySet();
        for (String interestedKey : interestedKeys) {
            if (grants.get(interestedKey).getStatus() == OperationResultStatus.WRONG_SHARD) {
                grants.remove(interestedKey);
            }
        }
        final MultiGrant.Builder mgb = MultiGrant.newBuilder();
        mgb.setClientId(mg.getClientId());
        mgb.setServerId(mg.getServerId());
        mgb.putAllGrants(grants);
        return mgb.build();
        
    }

    private TransactionResult executeWriteTransactionBL(final Transaction transactionToExecute) {
        Map<String, MultiGrant> write1mutiGrants, write1RefusedMultiGrants;
        Set<Server> relevantServers = new HashSet<Server>();
        final List<Operation> transactionOperations = transactionToExecute.getOperationsList();
        for (final Operation ops : transactionOperations) {
            Set<Server> serverSet = clusterConfiguration.getServerSetStoringObject(ops.getOperand1());
            relevantServers.addAll(serverSet);
        }
     

        while (true) {


            Random rand = new Random();
            final Write1ToServer.Builder write1toServerBuilder = Write1ToServer.newBuilder();
            /*
             * We dont have to send value in write1 message. Lets rebuild
             * transaction
             */
            final TransactionBuilder tb = TransactionBuilder.startNewTransaction();
            List<Operation> transactionOps = transactionToExecute.getOperationsList();
            for (final Operation op : transactionOps) {
                tb.addWriteWithoutValueOperation(op.getOperand1());
            }
            write1toServerBuilder.setTransaction(tb.build());
            write1toServerBuilder.setSeed(rand.nextInt(1000));
            write1toServerBuilder.setTransactionHash(Utils.objectSHA512(transactionToExecute));
            final List<Future<ProtocolMessage>> write1responseFutures = Utils.sendMessageToServers(
                    write1toServerBuilder, relevantServers, mochiMessaging);
            Utils.busyWaitForFutures(write1responseFutures);
            LOG.debug("Resolved write1response futures");
            final List<ProtocolMessage> write1responseProtocalMessages = Utils.getFutures(write1responseFutures);
    
            final List<Object> messages1FromServers = new ArrayList<Object>(relevantServers.size());
            // TODO: consider majority of votes
            boolean allWriteOk = true;
            for (ProtocolMessage pm : write1responseProtocalMessages) {
                final Write1OkFromServer writeOkFromServer = pm.getWrite1OkFromServer();
                final Write1RefusedFromServer writeRefusedFromServer = pm.getWrite1RefusedFromServer();
                if (pm.getPayloadCase() == PayloadCase.WRITE1OKFROMSERVER) {
                    messages1FromServers.add(writeOkFromServer);
                } else if (pm.getPayloadCase() == PayloadCase.WRITE1REFUSEDFROMSERVER) {
                    allWriteOk = false;
                    messages1FromServers.add(writeRefusedFromServer);
                } else if (pm.getPayloadCase() == PayloadCase.REQUESTFAILEDFROMSERVER) {
                    allWriteOk = false;
                    throw new RequestFailedException();
                } else {
                    allWriteOk = false;
                    // TODO: handle other responses
                    Utils.assertNotNull(writeOkFromServer, "Expected write1ok from the server");
                }
            }
    
            write1mutiGrants = new HashMap<String, MultiGrant>();
            write1RefusedMultiGrants = new HashMap<String, MultiGrant>();
            
            for (Object messageFromServer : messages1FromServers) {
                if (messageFromServer instanceof Write1OkFromServer) {
                    MultiGrant mg = ((Write1OkFromServer) messageFromServer).getMultiGrant();
                    Utils.assertNotNull(mg, "server1MultiGrant is null");
                    Utils.assertNotNull(mg.getServerId(), "serverId is null");
                    write1mutiGrants.put(mg.getServerId(), removeWrongShardGrantFromMultiGrant(mg));
                } else if (messageFromServer instanceof Write1RefusedFromServer) {
                    MultiGrant mg = ((Write1RefusedFromServer) messageFromServer).getMultiGrant();
                    Utils.assertNotNull(mg, "server1MultiGrant is null");
                    Utils.assertNotNull(mg.getServerId(), "serverId is null");
                    write1RefusedMultiGrants.put(mg.getServerId(), removeWrongShardGrantFromMultiGrant(mg));
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            
            if (isUniformTimeStampInMultiGrants(write1mutiGrants, transactionToExecute) == false) {
                LOG.info("Going to retry as timestamps on multigrants did not match");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
    
            if (allWriteOk) {
                LOG.info("Got write gratns servers {}. Progressing to step 2. Multigrants: ", write1mutiGrants.keySet(),
                        write1mutiGrants.values());
                break;
            } else {
                LOG.info("Got refused grant from servers {} {} *Aborting* ", write1RefusedMultiGrants.keySet(),
                        write1RefusedMultiGrants.values());
                throw new RequestRefusedException();
            }
        }

        // Step 2:

        final WriteCertificate.Builder wcb = WriteCertificate.newBuilder();
        wcb.putAllGrants(write1mutiGrants);

        final Write2ToServer.Builder write2ToServerBuilder = Write2ToServer.newBuilder();
        write2ToServerBuilder.setWriteCertificate(wcb);
        write2ToServerBuilder.setTransaction(transactionToExecute);

        final List<Future<ProtocolMessage>> write2responseFutures = Utils.sendMessageToServers(write2ToServerBuilder,
                relevantServers, mochiMessaging);
        Utils.busyWaitForFutures(write2responseFutures);
        LOG.debug("Resolved write2response futures");
        final List<ProtocolMessage> write2responseProtocolMessages = Utils.getFutures(write2responseFutures);

        final List<Write2AnsFromServer> write2ansFromServers = new ArrayList<Write2AnsFromServer>(
                relevantServers.size());
        for (final ProtocolMessage pm : write2responseProtocolMessages) {
            final Write2AnsFromServer write2AnsFromServer = pm.getWrite2AnsFromServer();
            Utils.assertNotNull(write2AnsFromServer, "write2AnsFromServer is null");
            write2ansFromServers.add(write2AnsFromServer);
        }
        
        
        int[] consistentTRCount = new int[transactionOperations.size()];
        List<OperationResult> coalescedResult = new ArrayList<OperationResult>(transactionOperations.size());
        
        for (int index = 0; index < transactionOperations.size(); index++) {
            coalescedResult.add(null);
            consistentTRCount[index] = 0;
        }
        
        for (Write2AnsFromServer response : write2ansFromServers) {
            TransactionResult tr = response.getResult();
            final List<OperationResult> operations = tr.getOperationsList();
            if (operations.size() != transactionOperations.size()) {
                throw new InconsistentReadException();
            }
            for (int index = 0; index < transactionOperations.size(); index++) {
                OperationResult or = operations.get(index);
                if (or.getStatus() != OperationResultStatus.WRONG_SHARD) {
                    consistentTRCount[index] = consistentTRCount[index] + 1;
                    coalescedResult.set(index, or);
                }
            }
        }
        
        for (int index = 0; index < transactionOperations.size(); index++) {
            if (consistentTRCount[index] < clusterConfiguration.getServerMajority()) {
                throw new InconsistentWriteException();
            }
        }
                
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(coalescedResult);
        final TransactionResult transactionResult = transactionResultBuilder.build();
        return transactionResult;
    }

    private void executePhase2() {
        // TODO
    }

    @Override
    public void close() {
        if (metricsJMXreporter != null) {
            metricsJMXreporter.close();
        }
        mochiMessaging.close();
    }

}
