package edu.stanford.cs244b.mochi.server;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.client.TransactionBuilder;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;

public class UtilsTest {

    @Test
    public void testUUID() {
        final String uuid = Utils.getUUID();
        Assert.assertEquals(uuid.length(), 36);
    }

    @Test
    public void testObjectSHA512() {
        final Transaction tr1 = TransactionBuilder.startNewTransaction(10).addWriteOperation("key", "hello world")
                .addReadOperation("key").addReadOperation("key2").build();

        final Transaction tr2 = TransactionBuilder.startNewTransaction(10).addWriteOperation("key", "hello world")
                .addReadOperation("key").addReadOperation("key2").build();

        final Transaction tr3 = TransactionBuilder.startNewTransaction(10).addWriteOperation("key", "hello world")
                .addReadOperation("key2").addReadOperation("key3").build();

        final String shaTr1 = Utils.objectSHA512(tr1);
        final String shaTr2 = Utils.objectSHA512(tr2);
        final String shaTr3 = Utils.objectSHA512(tr3);
        Assert.assertEquals(shaTr1, shaTr2);
        Assert.assertNotEquals(shaTr1, shaTr3);
    }
}
