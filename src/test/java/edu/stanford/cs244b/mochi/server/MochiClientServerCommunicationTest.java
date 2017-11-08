package edu.stanford.cs244b.mochi.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
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
        HelloFromServer hfs = mm.sayHelloToServer("localhost", MochiServer.PORT);

        String messageFromClient = hfs.getClientMsg();
        Assert.assertEquals(messageFromClient, MochiClientHandler.CLIENT_HELLO_MESSAGE);

        mm.close();
    }
}
