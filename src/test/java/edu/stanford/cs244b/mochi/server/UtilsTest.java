package edu.stanford.cs244b.mochi.server;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {

    @Test
    public void testUUID() {
        final String uuid = Utils.getUUID();
        Assert.assertEquals(uuid.length(), 36);
    }
}
