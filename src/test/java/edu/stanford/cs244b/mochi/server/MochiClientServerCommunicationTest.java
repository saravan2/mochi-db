package edu.stanford.cs244b.mochi.server;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;

public class MochiClientServerCommunicationTest {

    @Test
    public void testHelloToFromServer() throws InterruptedException {
        MochiServer ms = new MochiServer();
        MochiClient mc = new MochiClient();
        ms.start();
        HelloFromServer hfs = mc.sayHello();

        String messageFromClient = hfs.getClientMsg();
        Assert.assertEquals(messageFromClient, MochiClientHandler.CLIENT_HELLO_MESSAGE);
    }
}
