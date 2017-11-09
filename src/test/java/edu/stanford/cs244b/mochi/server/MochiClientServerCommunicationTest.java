package edu.stanford.cs244b.mochi.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer2;
import edu.stanford.cs244b.mochi.server.messaging.ConnectionNotReadyException;
import edu.stanford.cs244b.mochi.server.messaging.MochiMessaging;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;
import edu.stanford.cs244b.mochi.server.messaging.Server;
import edu.stanford.cs244b.mochi.server.requesthandlers.HelloToServerRequestHandler;

public class MochiClientServerCommunicationTest {
    final static Logger LOG = LoggerFactory.getLogger(MochiClientServerCommunicationTest.class);

    @Test
    public void testHelloToFromServer() throws InterruptedException {
        MochiServer ms = new MochiServer();
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

    @Test(expectedExceptions = ConnectionNotReadyException.class)
    public void testConnectionNotKnownServer() {
        final MochiMessaging mm = new MochiMessaging();
        final HelloFromServer hfs = mm.sayHelloToServer(new Server("some_unknown_host", MochiServer.DEFAULT_PORT));

        mm.close();
    }

    @Test
    public void testHelloToFromServerMultiple() throws InterruptedException {
        final int serverPort1 = 8001;
        final int serverPort2 = 8002;
        MochiServer ms1 = new MochiServer(serverPort1);
        ms1.start();
        MochiServer ms2 = new MochiServer(serverPort2);
        ms2.start();
        LOG.info("Mochi servers started");

        final MochiMessaging mm = new MochiMessaging();
        mm.waitForConnectionToBeEstablished(ms1.toServer());
        mm.waitForConnectionToBeEstablished(ms2.toServer());

        for (int i = 0; i < 5; i++) {
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

            Assert.assertEquals(hfs21.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
            Assert.assertEquals(hfs22.getClientMsg(), MochiMessaging.CLIENT_HELLO_MESSAGE);
            Assert.assertEquals(hfs21.getMsg(), HelloToServerRequestHandler.HELLO_RESPONSE);
            Assert.assertEquals(hfs22.getMsg(), HelloToServerRequestHandler.HELLO_RESPONSE);
        }


        mm.close();
    }
}
