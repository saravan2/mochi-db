package edu.stanford.cs244b.mochi.server;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.client.MochiDBClient;
import edu.stanford.cs244b.mochi.client.RequestFailedException;
import edu.stanford.cs244b.mochi.client.TransactionBuilder;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;
import edu.stanford.cs244b.mochi.server.messaging.ConnectionNotReadyException;
import edu.stanford.cs244b.mochi.server.messaging.MochiMessaging;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;
import edu.stanford.cs244b.mochi.server.messaging.Server;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServer2RequestHandler;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServerRequestHandler;
import edu.stanford.cs244b.mochi.testingframework.InMemoryDSMochiContextImpl;
import edu.stanford.cs244b.mochi.testingframework.MochiVirtualCluster;

public class MochiClientServerCommunicationTest {
    final static Logger LOG = LoggerFactory.getLogger(MochiClientServerCommunicationTest.class);

    @Test
    public void testHelloToFromServer() throws InterruptedException {
        MochiServer ms = newMochiServer();
        ms.start();
        LOG.info("Mochi server started");

        final MochiMessaging mm = new MochiMessaging();

        HelloFromServer hfs = null;
        for (int i = 0; i < 10; i++) {
            LOG.debug("Trying to send helloToServer {}th time", i);
            try {
                hfs = mm.sayHelloToServer(ms.toServer());
                break;
            } catch (ConnectionNotReadyException cnrEx) {
                Thread.sleep(200);
            }
        }
        if (hfs == null) {
            Assert.fail("Connection did not get established");
        }
        String messageFromClient = hfs.getClientMsg();
        Assert.assertEquals(messageFromClient, MochiMessaging.CLIENT_HELLO_MESSAGE);

        mm.close();
    }

    @Test(expectedExceptions = { ConnectionNotReadyException.class, java.net.UnknownHostException.class })
    public void testConnectionNotKnownServer() {
        final MochiMessaging mm = new MochiMessaging();
        final HelloFromServer hfs = mm.sayHelloToServer(new Server("some_unknown_host", MochiServer.DEFAULT_PORT));

        mm.close();
    }

    @Test
    public void testHelloToFromServerMultiple() throws InterruptedException {
        final int serverPort1 = 8001;
        final int serverPort2 = 8002;
        MochiServer ms1 = newMochiServer(serverPort1);
        ms1.start();
        MochiServer ms2 = newMochiServer(serverPort2);
        ms2.start();
        LOG.info("Mochi servers started");

        final MochiMessaging mm = new MochiMessaging();
        mm.waitForConnectionToBeEstablished(ms1.toServer());
        mm.waitForConnectionToBeEstablished(ms2.toServer());

        for (int i = 0; i < 2; i++) {
            LOG.info("testHelloToFromServerMultiple iteration {}", i);
            HelloFromServer hfs1 = mm.sayHelloToServer(ms1.toServer());
            HelloFromServer hfs2 = mm.sayHelloToServer(ms2.toServer());
            Assert.assertTrue(hfs1 != null);
            Assert.assertTrue(hfs2 != null);
            HelloFromServer2 hfs21 = mm.sayHelloToServer2(ms1.toServer());
            HelloFromServer2 hfs22 = mm.sayHelloToServer2(ms2.toServer());
            Assert.assertTrue(hfs21 != null);
            Assert.assertTrue(hfs22 != null);

            Assert.assertEquals(hfs1.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
            Assert.assertEquals(hfs2.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
            Assert.assertEquals(hfs1.getMsg(), HelloToServerRequestHandler.HELLO_RESPONSE);
            Assert.assertEquals(hfs2.getMsg(), HelloToServerRequestHandler.HELLO_RESPONSE);

            Assert.assertEquals(hfs21.getClientMsg(), MochiMessaging.CLIENT_HELLO2_MESSAGE);
            Assert.assertEquals(hfs22.getClientMsg(), MochiMessaging.CLIENT_HELLO2_MESSAGE);
            Assert.assertEquals(hfs21.getMsg(), HelloToServer2RequestHandler.HELLO_RESPONSE);
            Assert.assertEquals(hfs22.getMsg(), HelloToServer2RequestHandler.HELLO_RESPONSE);
        }

        mm.close();
        ms1.close();
        ms2.close();
    }

    @Test
    public void testHelloToFromServerAsync() throws InterruptedException, ExecutionException {
        final int serverPort1 = 8001;
        MochiServer ms1 = newMochiServer(serverPort1);
        ms1.start();

        final MochiMessaging mm = new MochiMessaging();
        mm.waitForConnectionToBeEstablished(ms1.toServer());

        for (int i = 0; i < 2; i++) {
            LOG.info("testHelloToFromServerMultipleAsync iteration {}", i);
            Future<ProtocolMessage> hfsFuture1 = mm.sayHelloToServerAsync(getServerToTestAgainst(ms1.toServer()));
            Future<ProtocolMessage> hfsFuture2 = mm.sayHelloToServerAsync(getServerToTestAgainst(ms1.toServer()));
            Future<ProtocolMessage> hfsFuture3 = mm.sayHelloToServerAsync(getServerToTestAgainst(ms1.toServer()));
            Assert.assertTrue(hfsFuture1 != null);
            Assert.assertTrue(hfsFuture2 != null);
            Assert.assertTrue(hfsFuture3 != null);

            long waitStart = System.currentTimeMillis();
            while (true) {
                if (futureCancelledOrDone(hfsFuture1) && futureCancelledOrDone(hfsFuture2)
                        && futureCancelledOrDone(hfsFuture3)) {
                    break;
                }
                Thread.sleep(3);
            }
            long waitEnd = System.currentTimeMillis();
            long getStart = System.currentTimeMillis();
            // Those should be fast since futures were already resolved
            ProtocolMessage pm1 = hfsFuture1.get();
            ProtocolMessage pm2 = hfsFuture2.get();
            ProtocolMessage pm3 = hfsFuture3.get();
            long getEnd = System.currentTimeMillis();

            HelloFromServer hfs1 = pm1.getHelloFromServer();
            HelloFromServer hfs2 = pm2.getHelloFromServer();
            HelloFromServer hfs3 = pm3.getHelloFromServer();

            LOG.info("Got requests from the server. Took {} ms to wait for response and {} ms to get them",
                    (waitEnd - waitStart), (getEnd - getStart));
            Assert.assertEquals(hfs1.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
            Assert.assertEquals(hfs2.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
            Assert.assertEquals(hfs3.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
        }

        mm.close();
        ms1.close();
    }

    @Test
    public void testReadOperation() throws InterruptedException, ExecutionException {
        
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final MochiDBClient mochiDBclient = new MochiDBClient();
        mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
        mochiDBclient.waitForConnectionToBeEstablishedToServers();

        final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction(mochiDBclient.getNextOperationNumber())
                .addWriteOperation("DEMO_READ_KEY_1", "NEW_VALUE_FOR_KEY_1_TR_1")
                .addWriteOperation("DEMO_READ_KEY_2", "NEW_VALUE_FOR_KEY_2_TR_1");
        
        final TransactionResult transaction1result = mochiDBclient.executeWriteTransaction(tb1.build());
        Assert.assertNotNull(transaction1result);

        final List<OperationResult> operationList = transaction1result.getOperationsList();
        Assert.assertNotNull(operationList);
        Assert.assertTrue(operationList.size() == 2);
        final OperationResult or1tr1 = Utils.getOperationResult(transaction1result, 0);
        final OperationResult or2tr1 = Utils.getOperationResult(transaction1result, 1);
        Assert.assertNotNull(or1tr1);
        Assert.assertNotNull(or2tr1);

        Assert.assertNotNull(or1tr1.getCurrentCertificate(), "write ceritificate for op1 in transaction 1 is null");
        Assert.assertNotNull(or2tr1.getCurrentCertificate(), "write ceritificate for op2 in transaction 1 is null");
        Assert.assertTrue(StringUtils.isEmpty(or1tr1.getResult()));
        Assert.assertFalse(or1tr1.getExisted());
        Assert.assertTrue(StringUtils.isEmpty(or2tr1.getResult()));
        Assert.assertFalse(or2tr1.getExisted());
        
        LOG.info("Second write transaction executed succesfully. Executing read transaction");
      
        final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction(mochiDBclient.getNextOperationNumber())
                .addReadOperation("DEMO_READ_KEY_1")
                .addReadOperation("DEMO_READ_KEY_2");

        
        final TransactionResult transaction2result = mochiDBclient.executeReadTransaction(tb2.build());
        Assert.assertNotNull(transaction2result);
        final List<OperationResult> operationList2 = transaction2result.getOperationsList();
        Assert.assertNotNull(operationList2);
        Assert.assertTrue(operationList2.size() == 2,
                String.format("Wrong size of operation list = %s", operationList2.size()));
        final OperationResult or1tr2 = Utils.getOperationResult(transaction2result, 0);
        final OperationResult or2tr2 = Utils.getOperationResult(transaction2result, 1);
        Assert.assertEquals(or1tr2.getResult(), "NEW_VALUE_FOR_KEY_1_TR_1");
        Assert.assertEquals(or2tr2.getResult(), "NEW_VALUE_FOR_KEY_2_TR_1");
        LOG.info("Read transaction executed successfully");
        
    }

    @Test
    public void testWriteOperation() throws InterruptedException, ExecutionException {
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final MochiDBClient mochiDBclient = new MochiDBClient();
        mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
        mochiDBclient.waitForConnectionToBeEstablishedToServers();

        final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction(mochiDBclient.getNextOperationNumber())
                .addWriteOperation("DEMO_KEY_1", "NEW_VALUE_FOR_KEY_1_TR_1")
                .addWriteOperation("DEMO_KEY_2", "NEW_VALUE_FOR_KEY_2_TR_1");
        
        final TransactionResult transaction1result = mochiDBclient.executeWriteTransaction(tb1.build());
        Assert.assertNotNull(transaction1result);

        final List<OperationResult> operationList = transaction1result.getOperationsList();
        Assert.assertNotNull(operationList);
        Assert.assertTrue(operationList.size() == 2);
        final OperationResult or1tr1 = Utils.getOperationResult(transaction1result, 0);
        final OperationResult or2tr1 = Utils.getOperationResult(transaction1result, 1);
        Assert.assertNotNull(or1tr1);
        Assert.assertNotNull(or2tr1);

        Assert.assertNotNull(or1tr1.getCurrentCertificate(), "write ceritificate for op1 in transaction 1 is null");
        Assert.assertNotNull(or2tr1.getCurrentCertificate(), "write ceritificate for op2 in transaction 1 is null");
        Assert.assertTrue(StringUtils.isEmpty(or1tr1.getResult()));
        Assert.assertFalse(or1tr1.getExisted());
        Assert.assertTrue(StringUtils.isEmpty(or2tr1.getResult()));
        Assert.assertFalse(or2tr1.getExisted());

        LOG.info("First write transaction executed successfully. Executing second write transaction");
        final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction(mochiDBclient.getNextOperationNumber())
                .addWriteOperation("DEMO_KEY_1", "NEW_VALUE_FOR_KEY_1_TR_2")
                .addWriteOperation("DEMO_KEY_2", "NEW_VALUE_FOR_KEY_2_TR_2");

        final TransactionResult transaction2result = mochiDBclient.executeWriteTransaction(tb2.build());
        Assert.assertNotNull(transaction2result);
        final List<OperationResult> operationList2 = transaction2result.getOperationsList();
        Assert.assertNotNull(operationList2);
        Assert.assertTrue(operationList2.size() == 2,
                String.format("Wrong size of operation list = %s", operationList2.size()));
        final OperationResult or1tr2 = Utils.getOperationResult(transaction2result, 0);
        final OperationResult or2tr2 = Utils.getOperationResult(transaction2result, 1);
        Assert.assertEquals(or1tr2.getResult(), "NEW_VALUE_FOR_KEY_1_TR_1");
        Assert.assertTrue(or1tr2.getExisted());
        Assert.assertEquals(or2tr2.getResult(), "NEW_VALUE_FOR_KEY_2_TR_1");
        Assert.assertTrue(or2tr2.getExisted());

        LOG.info("Second write transaction executed succesfully. Executing read transaction");
      
        final TransactionBuilder tb3 = TransactionBuilder.startNewTransaction(mochiDBclient.getNextOperationNumber())
                .addReadOperation("DEMO_KEY_1")
                .addReadOperation("DEMO_KEY_2");

        
        final TransactionResult transaction3result = mochiDBclient.executeReadTransaction(tb3.build());
        Assert.assertNotNull(transaction3result);
        final List<OperationResult> operationList3 = transaction3result.getOperationsList();
        Assert.assertNotNull(operationList3);
        Assert.assertTrue(operationList3.size() == 2,
                String.format("Wrong size of operation list = %s", operationList3.size()));
        final OperationResult or1tr3 = Utils.getOperationResult(transaction3result, 0);
        final OperationResult or2tr3 = Utils.getOperationResult(transaction3result, 1);
        Assert.assertEquals(or1tr3.getResult(), "NEW_VALUE_FOR_KEY_1_TR_2");
        Assert.assertEquals(or2tr3.getResult(), "NEW_VALUE_FOR_KEY_2_TR_2");
        LOG.info("Read transaction executed successfully");
        
        

        mochiVirtualCluster.close();
        mochiDBclient.close();
    }

    @Test(expectedExceptions = RequestFailedException.class)
    public void testWriteOperationTooOld() throws InterruptedException, ExecutionException {
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final MochiDBClient mochiDBclient = new MochiDBClient();
        mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
        mochiDBclient.waitForConnectionToBeEstablishedToServers();

        try {
            final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction(
                    mochiDBclient.getNextOperationNumber()).addWriteOperation("DEMO_KEY_1", "NEW_VALUE_FOR_KEY_1_TR_1");

            final TransactionResult transaction1result = mochiDBclient.executeWriteTransaction(tb1.build());
            Assert.assertNotNull(transaction1result);

            final List<OperationResult> operationList = transaction1result.getOperationsList();
            Assert.assertTrue(operationList.size() == 1);
            final OperationResult or1tr1 = Utils.getOperationResult(transaction1result, 0);
            Assert.assertNotNull(or1tr1.getCurrentCertificate(), "write ceritificate for op1 in transaction 1 is null");

            LOG.info("First write transaction executed successfully. "
                    + " Executing second write transaction which should raise exception");
            final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction(
                    mochiDBclient.getNextOperationNumber() - 2).addWriteOperation("DEMO_KEY_1",
                    "NEW_VALUE_FOR_KEY_1_TR_2");

            final TransactionResult transaction2result = mochiDBclient.executeWriteTransaction(tb2.build());

        } finally {
            mochiVirtualCluster.close();
            mochiDBclient.close();
        }
    }

    /*
     * Specify the remote server, port via command line arguments
     * -DremoteServer=###### -DremotePort=####
     */
    private Server getServerToTestAgainst(final Server server) {
        final String remoteServer = System.getProperty("remoteServer", null);
        final String remotePort = System.getProperty("remotePort", null);
        if (StringUtils.isEmpty(remoteServer) && StringUtils.isEmpty(remotePort)) {
            LOG.info("Using default server and port");
            return server;
        } else if (StringUtils.isEmpty(remotePort)) {
            LOG.info("Using remote server: {}", remoteServer);
            LOG.info("Using default port 8081");
            return new Server(remoteServer, 8081);
        } else {
            LOG.info("Using remote server: {}", remoteServer);
            LOG.info("Using remote port: {}", remotePort);
            return new Server(remoteServer, Integer.valueOf(remotePort));
        }
    }

    private boolean futureCancelledOrDone(Future<?> f) {
        return (f.isCancelled() || f.isDone());
    }

    private MochiServer newMochiServer() {
        return new MochiServer(new InMemoryDSMochiContextImpl());
    }

    private MochiServer newMochiServer(final int serverPort) {
        return new MochiServer(serverPort, new InMemoryDSMochiContextImpl());
    }
}
