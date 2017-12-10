package edu.stanford.cs244b.mochi.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.stanford.cs244b.mochi.testingframework.TestUtils;

public class ClusterConfigurationTest {
    final static Logger LOG = LoggerFactory.getLogger(ClusterConfigurationTest.class);

    @Test
    public void testTokenNumberToTokenValue() {
        ClusterConfiguration cc = new ClusterConfiguration();
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

    @Test
    public void testPutTokensAroundRingProps() {
        ClusterConfiguration cc = new ClusterConfiguration();
        final Properties props = new Properties();

        final List<String> servers = new ArrayList<String>();
        servers.add("a");
        servers.add("b");
        servers.add("c");
        servers.add("d");
        final Map<String, String> tokenProperties = cc.putTokensAroundRingProps(servers);
        final String constructedPropertyServers = tokenProperties.get(ClusterConfiguration.PROPERTY_SERVERS);
        Assert.assertNotNull(constructedPropertyServers);
        Assert.assertEquals(constructedPropertyServers, "a,b,c,d");
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_a_TOKENS"));
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_b_TOKENS"));
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_c_TOKENS"));
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_d_TOKENS"));
        props.putAll(tokenProperties);
        props.put(ClusterConfiguration.PROPERTY_BFT_REPLICATION, Integer.toString(servers.size()));
        props.put(String.format(ClusterConfiguration.PROPERTY_URL_SERVERS, "a"), "s_a:1");
        props.put(String.format(ClusterConfiguration.PROPERTY_URL_SERVERS, "b"), "s_b:1");
        props.put(String.format(ClusterConfiguration.PROPERTY_URL_SERVERS, "c"), "s_c:1");
        props.put(String.format(ClusterConfiguration.PROPERTY_URL_SERVERS, "d"), "s_d:1");

        cc.loadInitialConfigurationFromProperties(props);

        final String server = cc.getServerForObject("hello");
        final String[] abcd = { "a", "b", "c", "d" };
        TestUtils.assertIn(server, abcd);

        final String server2 = cc.getServerForObject("hello2");
        TestUtils.assertIn(server2, abcd);

        // Checking distribution
        final int numberOfRandomStringsToCheck = 200;
        final int minNumberOfServerOccurences = 20;
        Assert.assertTrue(minNumberOfServerOccurences < (numberOfRandomStringsToCheck / servers.size()));
        final Map<String, Integer> serversToTokenCounter = new HashMap<String, Integer>(servers.size());
        for (int i = 0; i < numberOfRandomStringsToCheck; i++) {
            final String randObject = RandomStringUtils.random(10);
            final String s = cc.getServerForObject(randObject);
            if (serversToTokenCounter.containsKey(s) == false) {
                serversToTokenCounter.put(s, 0);
            }
            serversToTokenCounter.put(s, serversToTokenCounter.get(s) + 1);
        }
        LOG.info("Random token distribution stats: {}", serversToTokenCounter);
        for (final String s : serversToTokenCounter.keySet()) {
            Assert.assertTrue(serversToTokenCounter.get(s) >= minNumberOfServerOccurences, String.format(
                    "Too little tokens given to server {}: {}, expected at least {}", s, serversToTokenCounter.get(s),
                    minNumberOfServerOccurences));
        }

        props.put(ClusterConfiguration.PROPERTY_BFT_REPLICATION, "4");
        cc.loadInitialConfigurationFromProperties(props);
        
        Assert.assertEquals(cc.getServerMajority(), 3);
        
        // 3 * 2 + 1
        props.put(ClusterConfiguration.PROPERTY_BFT_REPLICATION, "7");
        cc.loadInitialConfigurationFromProperties(props);
        // 2 * 2 + 1 = 5
        Assert.assertEquals(cc.getServerMajority(), 5);

    }

}
