package edu.stanford.cs244b.mochi.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messaging.ConnectionNotReadyException;
import edu.stanford.cs244b.mochi.server.messaging.MochiClientHandler;
import edu.stanford.cs244b.mochi.server.messaging.MochiMessaging;
import edu.stanford.cs244b.mochi.server.messaging.MochiServer;

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
                hfs = mm.sayHelloToServer("localhost", MochiServer.PORT);
                break;
            } catch (ConnectionNotReadyException cnrEx) {
                Thread.sleep(200);
            }
        }
        if (hfs == null) {
            Assert.fail("Connection did not get established");
        }
        String messageFromClient = hfs.getClientMsg();
        Assert.assertEquals(messageFromClient, MochiClientHandler.CLIENT_HELLO_MESSAGE);

        mm.close();
    }

    @Test(expectedExceptions = ConnectionNotReadyException.class)
    public void testConnectionNotKnownServer() {
        final MochiMessaging mm = new MochiMessaging();
        final HelloFromServer hfs = mm.sayHelloToServer("some_unknown_host", MochiServer.PORT);

        mm.close();
    }
}
