package edu.stanford.cs244b.mochi.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.client.MochiDBClient;
import edu.stanford.cs244b.mochi.client.RequestFailedException;
import edu.stanford.cs244b.mochi.client.RequestRefusedException;
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

    // That test is very simple, to save time, I comment it out
    @Test(enabled = false)
    public void testHelloToFromServer() throws InterruptedException {
        MochiServer ms = newMochiServer();
        ms.start();
        LOG.info("Mochi server started");

        final MochiMessaging mm = new MochiMessaging("testHelloToFromServer");

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
        final MochiMessaging mm = new MochiMessaging("testConnectionNotKnownServer");
        try {
            final HelloFromServer hfs = mm.sayHelloToServer(new Server("some_unknown_host", MochiServer.DEFAULT_PORT));
        } finally {
            mm.close();
        }
    }

    @Test
    public void testHelloToFromServerMultiple() throws InterruptedException {
        final int serverPort1 = 8001;
        final int serverPort2 = 8002;
        final MochiServer ms1 = newMochiServer(serverPort1);
        final MochiServer ms2 = newMochiServer(serverPort2);
        final MochiMessaging mm = new MochiMessaging("testHelloToFromServerMultiple");

        try {
            ms1.start();
            ms2.start();
            LOG.info("Mochi servers started");

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
        } finally {

            mm.close();
            ms1.close();
            ms2.close();
        }
    }

    @Test(dependsOnMethods = { "testHelloToFromServerMultiple" })
    public void testHelloToFromServerAsync() throws InterruptedException, ExecutionException {
        final int serverPort1 = 8001;
        MochiServer ms1 = newMochiServer(serverPort1);
        ms1.start();

        final MochiMessaging mm = new MochiMessaging("testHelloToFromServerAsync");
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

    @Test(dependsOnMethods = { "testHelloToFromServerAsync" })
    public void testReadOperation() throws InterruptedException, ExecutionException {
        
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final MochiDBClient mochiDBclient = new MochiDBClient();
        mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
        mochiDBclient.waitForConnectionToBeEstablishedToServers();

        final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction()
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
        Assert.assertEquals(or1tr1.getResult(), "NEW_VALUE_FOR_KEY_1_TR_1");
        Assert.assertEquals(or2tr1.getResult(), "NEW_VALUE_FOR_KEY_2_TR_1");


        Assert.assertNotNull(or1tr1.getCurrentCertificate(), "write ceritificate for op1 in transaction 1 is null");
        Assert.assertNotNull(or2tr1.getCurrentCertificate(), "write ceritificate for op2 in transaction 1 is null");
        Assert.assertTrue(or1tr1.getExisted());
        Assert.assertTrue(or2tr1.getExisted());
        
        LOG.info("Second write transaction executed succesfully. Executing read transaction");
      
        final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction()
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
        
        mochiVirtualCluster.close();
        mochiDBclient.close();
    }
    
    @Test(dependsOnMethods = { "testReadOperation" })
    public void testDeleteOperation() throws InterruptedException, ExecutionException {
        
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final MochiDBClient mochiDBclient = new MochiDBClient();
        mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
        mochiDBclient.waitForConnectionToBeEstablishedToServers();

        final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction()
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
        Assert.assertEquals(or1tr1.getResult(), "NEW_VALUE_FOR_KEY_1_TR_1");
        Assert.assertEquals(or2tr1.getResult(), "NEW_VALUE_FOR_KEY_2_TR_1");


        Assert.assertNotNull(or1tr1.getCurrentCertificate(), "write ceritificate for op1 in transaction 1 is null");
        Assert.assertNotNull(or2tr1.getCurrentCertificate(), "write ceritificate for op2 in transaction 1 is null");
        Assert.assertTrue(or1tr1.getExisted());
        Assert.assertTrue(or2tr1.getExisted());
        
        LOG.info("Write transaction executed succesfully. Executing read transaction");
      
        final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction()
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
        
        final TransactionBuilder tb3 = TransactionBuilder.startNewTransaction()
                .addDeleteOperation("DEMO_READ_KEY_1")
                .addDeleteOperation("DEMO_READ_KEY_2");
        
        final TransactionResult transaction3result = mochiDBclient.executeWriteTransaction(tb3.build());
        Assert.assertNotNull(transaction3result);
        final List<OperationResult> operationList3 = transaction3result.getOperationsList();
        Assert.assertNotNull(operationList3);
        
        Assert.assertTrue(operationList3.size() == 2);
        final OperationResult or1tr3 = Utils.getOperationResult(transaction3result, 0);
        final OperationResult or2tr3 = Utils.getOperationResult(transaction3result, 1);
        LOG.info("SAR Delete transaction result 1 {}\n SAR Delete transaction result 2 {}", or1tr3, or2tr3);
        Assert.assertNotNull(or1tr3);
        Assert.assertNotNull(or2tr3);
        Assert.assertTrue(or1tr3.getResult().isEmpty());
        Assert.assertTrue(or2tr3.getResult().isEmpty());
        Assert.assertFalse(or1tr3.getExisted());
        Assert.assertFalse(or2tr3.getExisted());
    
        LOG.info("Delete transaction executed successfully");
        
        final TransactionBuilder tb4 = TransactionBuilder.startNewTransaction()
                .addReadOperation("DEMO_READ_KEY_1")
                .addReadOperation("DEMO_READ_KEY_2");
        
        final TransactionResult transaction4result = mochiDBclient.executeReadTransaction(tb4.build());
        Assert.assertNotNull(transaction4result);
        final List<OperationResult> operationList4 = transaction4result.getOperationsList();
        Assert.assertNotNull(operationList4);
        Assert.assertTrue(operationList4.size() == 2,
                String.format("Wrong size of operation list = %s", operationList4.size()));
        final OperationResult or1tr4 = Utils.getOperationResult(transaction4result, 0);
        final OperationResult or2tr4 = Utils.getOperationResult(transaction4result, 1);
        Assert.assertNotNull(or1tr4);
        Assert.assertNotNull(or2tr4);
        Assert.assertTrue(or1tr4.getResult().isEmpty());
        Assert.assertTrue(or2tr4.getResult().isEmpty());
        Assert.assertFalse(or1tr4.getExisted());
        Assert.assertFalse(or2tr4.getExisted());
        
        LOG.info("Read transaction on deleted objects finished successfully");
        
        mochiVirtualCluster.close();
        mochiDBclient.close();
    }

    @Test(dependsOnMethods = { "testDeleteOperation" })
    public void testWriteOperation() throws InterruptedException, ExecutionException {
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final MochiDBClient mochiDBclient = new MochiDBClient();
        mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
        mochiDBclient.waitForConnectionToBeEstablishedToServers();

        final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction()
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
        Assert.assertEquals(or1tr1.getResult(), "NEW_VALUE_FOR_KEY_1_TR_1");
        Assert.assertEquals(or2tr1.getResult(), "NEW_VALUE_FOR_KEY_2_TR_1");

        Assert.assertNotNull(or1tr1.getCurrentCertificate(), "write ceritificate for op1 in transaction 1 is null");
        Assert.assertNotNull(or2tr1.getCurrentCertificate(), "write ceritificate for op2 in transaction 1 is null");
        Assert.assertTrue(or1tr1.getExisted());
        Assert.assertTrue(or2tr1.getExisted());

        LOG.info("First write transaction executed successfully. Executing second write transaction");
        final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction()
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
        Assert.assertEquals(or1tr2.getResult(), "NEW_VALUE_FOR_KEY_1_TR_2");
        Assert.assertTrue(or1tr2.getExisted());
        Assert.assertEquals(or2tr2.getResult(), "NEW_VALUE_FOR_KEY_2_TR_2");
        Assert.assertTrue(or2tr2.getExisted());

        LOG.info("Second write transaction executed succesfully. Executing read transaction");
      
        final TransactionBuilder tb3 = TransactionBuilder.startNewTransaction()
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
    
    @Test(dependsOnMethods = { "testWriteOperation" })
    // TODO Most of the asserts in concurrent client test case are invalid. We should make
    // our test clients accept write and read failures.
    public void testWriteOperationConcurrent() throws InterruptedException, ExecutionException {
        LOG.debug("Starting testWriteOperationConcurrent");
        final int numberOfServersToTest = 4;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final int numberOfCurrentClients = 5;
        final List<MochiDBClient> clients = new ArrayList<MochiDBClient>(numberOfCurrentClients);
        final List<MochiConcurrentTestRunnable> runnables = new ArrayList<MochiConcurrentTestRunnable>(
                numberOfCurrentClients);
        for (int i = 0; i < numberOfCurrentClients; i++) {
            final MochiDBClient mochiDBclient = new MochiDBClient();
            mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
            mochiDBclient.waitForConnectionToBeEstablishedToServers();

            clients.add(mochiDBclient);
            final MochiConcurrentTestRunnable mctr = new MochiConcurrentTestRunnable(mochiDBclient);
            runnables.add(mctr);
        }
        LOG.info("Concurrent test: add variables have been initialized");

        for (final MochiDBClient c : clients) {
            c.waitForConnectionToBeEstablishedToServers();
        }
        LOG.info("Concurrent test: connection was established. Trying one by one");

        int i = 0;
        for (final MochiConcurrentTestRunnable r : runnables) {
            r.run();
            LOG.info("Succeeded operation for client {}", i);
            i++;
            Assert.assertTrue(r.getTestPassed());
        }

        // Thread.sleep(120 * 1000);
        LOG.info("Now executing test concurrently");

        final ExecutorService threadPoolExecutor = new ThreadPoolExecutor(numberOfCurrentClients,
                numberOfCurrentClients, 360, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        final List<Future<Void>> futures = new ArrayList<Future<Void>>(numberOfCurrentClients);
        for (final Runnable r : runnables) {
            final Future<?> f = threadPoolExecutor.submit(r);
            futures.add((Future<Void>) f);
        }

        Utils.busyWaitForFutures(futures);

        for (final MochiConcurrentTestRunnable r : runnables) {
            LOG.info("Succeeded operation for client {}", r.mochiDBclient.getClientID());
            Assert.assertTrue(r.getTestPassed());
        }
        mochiVirtualCluster.close();
        for (final MochiDBClient c : clients) {
            c.close();
        }
    }
    
    private class MochiConcurrentTestRunnable implements Runnable {
        private final MochiDBClient mochiDBclient;
        private volatile Boolean testPassed = null;
        
        public MochiConcurrentTestRunnable(MochiDBClient mochiDBclient) {
            this.mochiDBclient = mochiDBclient;
        }


        public void run() {
            LOG.debug("MochiConcurrentTestRunnable starting test for client {}", mochiDBclient.getClientID());
            testPassed = null;
            try {
                runTest();
                LOG.info("MochiConcurrentTestRunnable: Passed test mochiClient {}", mochiDBclient.getClientID());
                testPassed = true;
            } catch (Exception ex) {
                LOG.info("MochiConcurrentTestRunnable: Test failed for mochiClient {}", mochiDBclient.getClientID());
                testPassed = false;
                throw new RuntimeException(ex);
            }
        }

        public void runTest() {
            LOG.info("Concurrent Client {} starts test", mochiDBclient.getClientID());
            final TransactionBuilder tb1 = TransactionBuilder.startNewTransaction()
                    .addWriteOperation("DEMO_KEY_1", "NEW_VALUE_FOR_KEY_1_TR_1")
                    .addWriteOperation("DEMO_KEY_2", "NEW_VALUE_FOR_KEY_2_TR_1");
            TransactionResult transaction1result = null;
            try {
                transaction1result  = mochiDBclient.executeWriteTransaction(tb1.build());
            } catch (RequestRefusedException Ex) {
                LOG.info("Concurrent Client {} First write transaction's write1 request was refused", mochiDBclient.getClientID());
                throw new RuntimeException(Ex);
            } catch (RequestFailedException Ex) {
                LOG.info("Concurrent Client {} First write transaction's write1 request failed", mochiDBclient.getClientID());
                throw new RuntimeException(Ex);
            }
            Assert.assertNotNull(transaction1result);

            final List<OperationResult> operationList = transaction1result.getOperationsList();
            Assert.assertNotNull(operationList);
            Assert.assertTrue(operationList.size() == 2,
                    String.format("Wrong size of operation list = %s on Concurrent Client {}. Objects could have been deleted by concurrent write", mochiDBclient.getClientID(), operationList.size()));
            
            final OperationResult or1tr1 = Utils.getOperationResult(transaction1result, 0);
            final OperationResult or2tr1 = Utils.getOperationResult(transaction1result, 1);
            Assert.assertNotNull(or1tr1);
            Assert.assertNotNull(or2tr1);
            Assert.assertEquals(or1tr1.getResult(), "NEW_VALUE_FOR_KEY_1_TR_1", String.format("Concurrent Client {} Write value NEW_VALUE_FOR_KEY_1_TR_1 has been overwritten with {}", mochiDBclient.getClientID(), or1tr1.getResult()));
            Assert.assertEquals(or2tr1.getResult(), "NEW_VALUE_FOR_KEY_2_TR_1", String.format("Concurrent Client {} Write value NEW_VALUE_FOR_KEY_2_TR_1 has been overwritten with {}", mochiDBclient.getClientID(), or2tr1.getResult()));

            Assert.assertNotNull(or1tr1.getCurrentCertificate(),
                    String.format("Concurrent Client {} write ceritificate for op1 in transaction 1 is null", mochiDBclient.getClientID()));
            Assert.assertNotNull(or2tr1.getCurrentCertificate(),
                    String.format("Concurrent Client {} write ceritificate for op2 in transaction 1 is null", mochiDBclient.getClientID()));

            LOG.info("Concurrent Client {} First write transaction executed successfully. Executing second write transaction", mochiDBclient.getClientID());
            final TransactionBuilder tb2 = TransactionBuilder.startNewTransaction()
                    .addWriteOperation("DEMO_KEY_1", "NEW_VALUE_FOR_KEY_1_TR_2")
                    .addWriteOperation("DEMO_KEY_2", "NEW_VALUE_FOR_KEY_2_TR_2");
            TransactionResult transaction2result = null;
            try {
                transaction2result = mochiDBclient.executeWriteTransaction(tb2.build());
            } catch (RequestRefusedException Ex) {
                LOG.info("Concurrent Client {} Second write transaction's write1 request was refused", mochiDBclient.getClientID());
                throw new RuntimeException(Ex);
            } catch (RequestFailedException Ex) {
                LOG.info("Concurrent Client {} Second write transaction's write1 request failed", mochiDBclient.getClientID());
                throw new RuntimeException(Ex);
            }
            Assert.assertNotNull(transaction2result);
            final List<OperationResult> operationList2 = transaction2result.getOperationsList();
            Assert.assertNotNull(operationList2);
            Assert.assertTrue(operationList2.size() == 2,
                    String.format("Wrong size of operation list = %s on Concurrent Client {}. Objects could have been deleted by concurrent write", mochiDBclient.getClientID(), operationList2.size()));
             
            final OperationResult or1tr2 = Utils.getOperationResult(transaction2result, 0);
            final OperationResult or2tr2 = Utils.getOperationResult(transaction2result, 1);
            Assert.assertEquals(or1tr2.getResult(), "NEW_VALUE_FOR_KEY_1_TR_2", String.format("Concurrent Client {} Write value NEW_VALUE_FOR_KEY_1_TR_2 has been overwritten with {}", mochiDBclient.getClientID(), or1tr2.getResult()));
            Assert.assertTrue(or1tr2.getExisted());
            Assert.assertEquals(or2tr2.getResult(), "NEW_VALUE_FOR_KEY_2_TR_2", String.format("Concurrent Client {} Write value NEW_VALUE_FOR_KEY_2_TR_2 has been overwritten with {}", mochiDBclient.getClientID(), or2tr2.getResult()));
            Assert.assertTrue(or2tr2.getExisted());
            LOG.info("Concurrent Client {} Second write transaction executed succesfully. Executing read transaction", mochiDBclient.getClientID());
            final TransactionBuilder tb3 = TransactionBuilder.startNewTransaction().addReadOperation("DEMO_KEY_1")
                    .addReadOperation("DEMO_KEY_2");

            final TransactionResult transaction3result = mochiDBclient.executeReadTransaction(tb3.build());
            Assert.assertNotNull(transaction3result);
            final List<OperationResult> operationList3 = transaction3result.getOperationsList();
            Assert.assertNotNull(operationList3);
            Assert.assertTrue(operationList3.size() == 2,
                    String.format("Wrong size of operation list = %s on Concurrent Client {}. Objects could ould have been deleted by concurrent write", mochiDBclient.getClientID(), operationList3.size()));
            final OperationResult or1tr3 = Utils.getOperationResult(transaction3result, 0);
            final OperationResult or2tr3 = Utils.getOperationResult(transaction3result, 1);
            Assert.assertEquals(or1tr3.getResult(), "NEW_VALUE_FOR_KEY_1_TR_2",  String.format("Concurrent Client {} Expected Read value NEW_VALUE_FOR_KEY_1_TR_2 has been overwritten with {}", mochiDBclient.getClientID(), or1tr3.getResult()));
            Assert.assertEquals(or2tr3.getResult(), "NEW_VALUE_FOR_KEY_2_TR_2",  String.format("Concurrent Client {} Expected Read value NEW_VALUE_FOR_KEY_2_TR_2 has been overwritten with {}", mochiDBclient.getClientID(), or2tr3.getResult()));
            LOG.info("Concurrent Client {} Read transaction executed successfully", mochiDBclient.getClientID());

            LOG.info("Running more transactions");
            final TransactionBuilder tb4 = TransactionBuilder.startNewTransaction().addWriteOperation(
                    "DEMO_KEY_TEST_1", "1");
            final TransactionResult transaction4result = mochiDBclient.executeWriteTransaction(tb4.build());
            Assert.assertNotNull(transaction4result);

            final TransactionBuilder tb5 = TransactionBuilder.startNewTransaction().addWriteOperation(
                    "DEMO_KEY_TEST_2", "2");
            final TransactionResult transaction5result = mochiDBclient.executeWriteTransaction(tb5.build());
            Assert.assertNotNull(transaction5result);

            LOG.info("Concurrent Client {} ends test", mochiDBclient.getClientID());
        }

        public Boolean getTestPassed() {
            return testPassed;
        }
    };

    @Test(dependsOnMethods = { "testWriteOperationConcurrent" })
    public void testWriteOperationConcurrentStressTest() throws InterruptedException, ExecutionException {
        LOG.debug("Starting testWriteOperationConcurrentStressTest");
        final int numberOfServersToTest = 4;
        final int keyRange = 20;
        final MochiVirtualCluster mochiVirtualCluster = new MochiVirtualCluster(numberOfServersToTest, 1);
        mochiVirtualCluster.startAllServers();

        final int numberOfCurrentClients = 5;
        final List<MochiDBClient> clients = new ArrayList<MochiDBClient>(numberOfCurrentClients);
        final List<MochiConcurrentStreeTestRunnable> runnables = new ArrayList<MochiConcurrentStreeTestRunnable>(
                numberOfCurrentClients);
        int rangePos = 0;
        for (int i = 0; i < numberOfCurrentClients; i++) {
            final MochiDBClient mochiDBclient = new MochiDBClient();
            mochiDBclient.addServers(mochiVirtualCluster.getAllServers());
            mochiDBclient.waitForConnectionToBeEstablishedToServers();

            clients.add(mochiDBclient);
            final MochiConcurrentStreeTestRunnable mctr = new MochiConcurrentStreeTestRunnable(mochiDBclient, rangePos,
                    keyRange);
            runnables.add(mctr);
            rangePos += keyRange;
        }
        LOG.info("Concurrent test: add variables have been initialized");

        for (final MochiDBClient c : clients) {
            c.waitForConnectionToBeEstablishedToServers();
        }
        LOG.info("Concurrent test: connection was established. Trying one by one");

        final ExecutorService threadPoolExecutor = new ThreadPoolExecutor(numberOfCurrentClients,
                numberOfCurrentClients, 240, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        final List<Future<Void>> futures = new ArrayList<Future<Void>>(numberOfCurrentClients);
        for (final Runnable r : runnables) {
            final Future<?> f = threadPoolExecutor.submit(r);
            futures.add((Future<Void>) f);
        }

        Utils.busyWaitForFutures(futures);

        for (final MochiConcurrentStreeTestRunnable r : runnables) {
            LOG.info("Succeeded stress test operation for client {}", r.mochiDBclient.getClientID());
            Assert.assertTrue(r.getTestPassed());
        }

        // Thread.sleep(120 * 1000);
        mochiVirtualCluster.close();
        for (final MochiDBClient c : clients) {
            c.close();
        }
    }
    
    private class MochiConcurrentStreeTestRunnable implements Runnable {
        private final MochiDBClient mochiDBclient;
        private volatile Boolean testPassed = null;
        private final int initialNumberStart;
        private final int regionSize;
        private int keyEndingNums[];
        
        public MochiConcurrentStreeTestRunnable(MochiDBClient mochiDBclient, int initialNumberStart, int regionSize) {
            this.mochiDBclient = mochiDBclient;
            this.initialNumberStart = initialNumberStart;
            this.regionSize = regionSize;
            if (regionSize % 2 != 0) {
                throw new IllegalStateException("regionSize should be divisible by 2");
            }
            keyEndingNums = new int[regionSize];
            for (int i = 0; i < regionSize; i++) {
                keyEndingNums[i] = initialNumberStart + i;
            }
            shuffleArray(keyEndingNums);
        }


        public void run() {
            LOG.debug("MochiConcurrentTestRunnable starting test");
            testPassed = null;
            try {
                runTest();
                testPassed = true;
            } catch (Exception ex) {
                LOG.info("Test failed !!!");
                testPassed = false;
                throw new RuntimeException(ex);
            }
        }

        public void runTest() {
            LOG.info("Concurrent Client {} starts stree test", mochiDBclient.getClientID());

            LOG.info("Step 0: Executing writes to all keys single. Client {}", mochiDBclient.getClientID());
            for (int i = 0; i < keyEndingNums.length; i++) {
                final String key = String.format("DEMO_KEY_STRESS_TEST_%s", keyEndingNums[i]);
                final String val = String.format("New Value for key %s", key);
                final TransactionBuilder tb = TransactionBuilder.startNewTransaction().addWriteOperation(key, val);
                mochiDBclient.executeWriteTransaction(tb.build());
            }
            LOG.info("Step 0: finished. Client {}", mochiDBclient.getClientID());
            shuffleArray(keyEndingNums);
            LOG.info("Step 1: Executing reads");
            for (int i = 0; i < keyEndingNums.length; i++) {
                final String key = String.format("DEMO_KEY_STRESS_TEST_%s", keyEndingNums[i]);
                final String val = String.format("New Value for key %s", key);
                final TransactionBuilder tb = TransactionBuilder.startNewTransaction().addReadOperation(key);
                TransactionResult tr = mochiDBclient.executeReadTransaction(tb.build());
                final String gotVal = tr.getOperationsList().get(0).getResult();
                Assert.assertEquals(gotVal, val);
            }
            LOG.info("Step 1 finished. Client {}", mochiDBclient.getClientID());
            // TODO: add reads
            LOG.info("Concurrent Client {} ends test", mochiDBclient.getClientID());
        }

        public Boolean getTestPassed() {
            return testPassed;
        }
    };

    private static void shuffleArray(int[] array) {
        int index;
        Random random = new Random();
        for (int i = array.length - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            if (index != i) {
                array[index] ^= array[i];
                array[i] ^= array[index];
                array[index] ^= array[i];
            }
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
