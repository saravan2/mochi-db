package edu.stanford.cs244b.mochi.server;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

public class ClusterConfiguration {
    public static final int SHARD_TOKENS = 1024;
    public static final long SHARD_TOKEN_VALUE_RANGE = getTokenValueRangePerToken(SHARD_TOKENS);
    public static final long MAX_INT = 0xffffffffl;

    public static final char CONFIG_DELIMITER = ',';
    public static final String PROPERTY_SERVERS = "_CONFIG_SERVERS";
    public static final String PROPERTY_PREF_SERVERS = "_CONFIG_SERVER_%s_TOKENS=";

    public ConcurrentHashMap<Long, String> tokensToServers;
    private final MochiContext mochiContext;

    public ClusterConfiguration(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
        tokensToServers = new ConcurrentHashMap<Long, String>(getShardTokens());
    }

    public void loadInitialConfigurationFromProperties(final Properties props) {
        final String[] allServers = splitMultiple(props.getProperty(PROPERTY_SERVERS, ""));
        for (final String s : allServers) {
            final String[] tokensForServer = splitMultiple(props.getProperty(String.format(PROPERTY_PREF_SERVERS, s), ""));
            for (final String tokenS : tokensForServer) {
                Integer tokenNumber = Integer.parseInt(tokenS);
                if (tokenNumber >= SHARD_TOKENS) {
                    throw new IllegalStateException("Too large shard number");
                }
                Long token = tokenNumberToTokenValue(tokenNumber);
                if (tokensToServers.contains(token)) {
                    throw new IllegalStateException(String.format("Mutple mapping for token: %s (number %s)", token,
                            tokenNumber));
                }

            }
        }
        // Validating that all token have been assigned
        for (int i = 0; i < SHARD_TOKENS; i++) {
            Long token = tokenNumberToTokenValue(i);
            if (tokensToServers.contains(token)) {
                throw new IllegalStateException(String.format("Token %s is not assigned", token));
            }
        }
    }
    
    public String getServerForObjectHashCode(int hashCode) {
        final Long hashCodeUnsigned = intToUnsignedLong(hashCode);
        final Long token = hashCodeUnsigned / SHARD_TOKEN_VALUE_RANGE;
        return getServerForToken(token);
    }

    public String getServerForToken(Long token) {
        final String serverForToken = tokensToServers.get(token);
        if (serverForToken == null) {
            throw new IllegalStateException(String.format("Failed to find server for token %s"));
        }
        return serverForToken;
    }

    public static long getTokenValueRangePerToken(final int shardTokens) {
        return (long) (MAX_INT / shardTokens);
    }

    public long tokenNumberToTokenValue(final Integer tokenNumber) {
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
