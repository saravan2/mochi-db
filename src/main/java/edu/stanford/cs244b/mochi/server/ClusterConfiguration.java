package edu.stanford.cs244b.mochi.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterConfiguration {
    private final static Logger LOG = LoggerFactory.getLogger(ClusterConfiguration.class);

    public static final int SHARD_TOKENS = 1024;
    public static final long SHARD_TOKEN_VALUE_RANGE = getTokenValueRangePerToken(SHARD_TOKENS);
    public static final long MAX_INT = 0xffffffffl;

    public static final char CONFIG_DELIMITER = ',';
    public static final String PROPERTY_SERVERS = "_CONFIG_SERVERS";
    public static final String PROPERTY_PREF_SERVERS = "_CONFIG_SERVER_%s_TOKENS";

    public ConcurrentHashMap<Long, String> tokensToServers;
    private final MochiContext mochiContext;

    public ClusterConfiguration(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
        tokensToServers = new ConcurrentHashMap<Long, String>(getShardTokens());
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
            secondaryIndex++;
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

    public void loadInitialConfigurationFromProperties(final Properties props) {
        LOG.debug("Loading initial configuration from properties {}", props);
        final String[] allServers = splitMultiple(props.getProperty(PROPERTY_SERVERS, ""));
        for (final String s : allServers) {
            final String[] tokensForServer = splitMultiple(props.getProperty(String.format(PROPERTY_PREF_SERVERS, s),
                    ""));
            LOG.debug("{} tokens are given to server {}", tokensForServer.length, s);
            for (final String tokenS : tokensForServer) {
                Integer tokenNumber = Integer.parseInt(tokenS);
                if (tokenNumber >= SHARD_TOKENS) {
                    throw new IllegalStateException("Too large shard number");
                }
                Long token = tokenNumberToTokenValue(tokenNumber);
                if (tokensToServers.containsKey(token)) {
                    throw new IllegalStateException(String.format("Mutple mapping for token: %s (number %s)", token,
                            tokenNumber));
                }
                tokensToServers.put(token, s);

            }
        }
        // Validating that all token have been assigned
        for (int i = 0; i < SHARD_TOKENS; i++) {
            Long token = tokenNumberToTokenValue(i);
            if (tokensToServers.containsKey(token) == false) {
                throw new IllegalStateException(String.format("Token %s (index %s) is not assigned among {} tokens",
                        token, i, tokensToServers.size()));
            }
        }
    }

    public String getServerForObject(final String object) {
        // TODO: remove hardcoded 4
        final List<String> servers = getServersForObjectHashCode(object.hashCode(), 4);
        return servers.get(0);
    }

    public List<String> getServersForObjectHashCode(int hashCode, int bftReplicationFactor) {
        Utils.assertTrue(bftReplicationFactor >= 4, "Replication factor for BFT should be >= 4");
        final Long hashCodeUnsigned = intToUnsignedLong(hashCode);
        final Integer tokenValStart = (int) (hashCodeUnsigned / SHARD_TOKEN_VALUE_RANGE);
        final List<String> servers = new ArrayList<String>(bftReplicationFactor);
        for (int i = 0; i < bftReplicationFactor; i++) {
            final int ithTokenValue = (tokenValStart + i) % SHARD_TOKENS;
            final Long token = tokenNumberToTokenValue(i);
            final String server = getServerForToken(token);
            if (servers.contains(server)) {
                throw new IllegalStateException(String.format(
                                "BFT requires all servers to be unique. Found collision for server %s. Replication factor %s, starting from token %s (%s). Collision token %s. All servers %s",
                                server, bftReplicationFactor, ithTokenValue, token, i, servers));
            }
            servers.add(server);
        }
        return servers;
    }

    public String getServerForToken(Long token) {
        final String serverForToken = tokensToServers.get(token);
        if (serverForToken == null) {
            throw new IllegalStateException(String.format("Failed to find server for token %s", token));
        }
        return serverForToken;
    }

    public static long getTokenValueRangePerToken(final int shardTokens) {
        return (long) (MAX_INT / shardTokens);
    }

    public Long tokenNumberToTokenValue(final Integer tokenNumber) {
        return tokenNumber * SHARD_TOKEN_VALUE_RANGE;
    }

    public static long intToUnsignedLong(final int signedInt) {
        return signedInt & MAX_INT;
    }

    public String[] splitMultiple(final String s) {
        return StringUtils.split(s, CONFIG_DELIMITER);
    }

    public int getShardTokens() {
        return SHARD_TOKENS;
    }
}
