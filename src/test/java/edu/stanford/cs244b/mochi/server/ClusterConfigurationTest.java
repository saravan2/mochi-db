package edu.stanford.cs244b.mochi.server;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterConfigurationTest {
    final static Logger LOG = LoggerFactory.getLogger(ClusterConfigurationTest.class);

    @Test
    public void testTokenNumberToTokenValue() {
        ClusterConfiguration cc = new ClusterConfiguration(Mockito.mock(MochiContext.class));
        Assert.assertTrue(ClusterConfiguration.SHARD_TOKEN_VALUE_RANGE > 0);

        int token1 = 0;
        long token1Value = cc.tokenNumberToTokenValue(token1);
        Assert.assertTrue(token1Value == 0);

        int token2 = 1;
        long token2Value = cc.tokenNumberToTokenValue(token2);
        Assert.assertTrue(token2Value == ClusterConfiguration.SHARD_TOKEN_VALUE_RANGE);

        int token3 = 2;
        long token3Value = cc.tokenNumberToTokenValue(token3);
        Assert.assertTrue(token3Value == ClusterConfiguration.SHARD_TOKEN_VALUE_RANGE * 2);

        int token4 = ClusterConfiguration.SHARD_TOKENS - 1;
        long token4Value = cc.tokenNumberToTokenValue(token4);
        final long maxInt = 0xffffffffl;
        Assert.assertTrue(token4Value < maxInt);
        Assert.assertTrue(token4Value + ClusterConfiguration.SHARD_TOKEN_VALUE_RANGE * 2 > maxInt);

        LOG.info("testTokenNumberToTokenValue: {} -> {}, {} -> {}, {} -> {}, {} -> {} (max {})", token1, token1Value,
                token2, token2Value, token3, token3Value, token4, token4Value, maxInt);
    }
}
