package edu.stanford.cs244b.mochi.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.tomcat.util.buf.StringUtils;
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

    public Map<String, String> putTokensAroundRingProps(final List<String> servers) {
        final int numberOfServers = servers.size();
        
        final List<List<String>> assignedTokensToServers = new ArrayList<List<String>>(numberOfServers);
        for (int i = 0; i < numberOfServers; i++) {
            assignedTokensToServers.add(new ArrayList<String>(ClusterConfiguration.SHARD_TOKENS / numberOfServers));
        }

        int secondaryIndex = 0;
        for (int i = 0; i < ClusterConfiguration.SHARD_TOKENS; i++) {
            if (secondaryIndex == numberOfServers) {
                secondaryIndex = 0;
            }
            final List<String> tokenForServerX = assignedTokensToServers.get(secondaryIndex);
            tokenForServerX.add(Integer.toString(i));
        }

        final Map<String, String> serverToTokensProps = new HashMap<String, String>();
        
        // Adding _CONFIG_SERVER_ property
        for (int i = 0; i < numberOfServers; i++) {
            final String propKey = String.format(ClusterConfiguration.PROPERTY_PREF_SERVERS, servers.get(i));
            final String propValue = StringUtils.join(assignedTokensToServers.get(i),
                    ClusterConfiguration.CONFIG_DELIMITER);
            serverToTokensProps.put(propKey, propValue);
        }
        // Adding property servers
        final String propServersValue = StringUtils.join(servers, ClusterConfiguration.CONFIG_DELIMITER);
        serverToTokensProps.put(ClusterConfiguration.PROPERTY_SERVERS, propServersValue);
        return serverToTokensProps;
    }

    @Test
    public void testPutTokensAroundRingProps() {
        ClusterConfiguration cc = new ClusterConfiguration(Mockito.mock(MochiContext.class));
        final Properties props = new Properties();

        final List<String> servers = new ArrayList<String>();
        servers.add("a");
        servers.add("b");
        servers.add("c");
        servers.add("d");
        final Map<String, String> tokenProperties = putTokensAroundRingProps(servers);
        final String constructedPropertyServers = tokenProperties.get(ClusterConfiguration.PROPERTY_SERVERS);
        Assert.assertNotNull(constructedPropertyServers);
        Assert.assertEquals(constructedPropertyServers, "a,b,c,d");
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_a_TOKENS"));
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_b_TOKENS"));
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_c_TOKENS"));
        Assert.assertNotNull(tokenProperties.get("_CONFIG_SERVER_d_TOKENS"));

        // cc.loadInitialConfigurationFromProperties(props);
        // TODO
        // cc.getServerForToken(token)
    }
}
