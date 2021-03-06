package edu.stanford.cs244b.mochi.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messaging.Server;

public class ClusterConfiguration {
    private final static Logger LOG = LoggerFactory.getLogger(ClusterConfiguration.class);

    public static final int SHARD_TOKENS = 1024;
    public static final long SHARD_TOKEN_VALUE_RANGE = getTokenValueRangePerToken(SHARD_TOKENS);
    public static final long MAX_INT = 0xffffffffl;

    public static final char CONFIG_DELIMITER = ',';
    public static final String PROPERTY_SERVERS = "_CONFIG_SERVERS";
    public static final String PROPERTY_BFT_REPLICATION = "_CONFIG_BFT_REPLICATION";
    public static final String PROPERTY_PREF_SERVERS = "_CONFIG_SERVER_%s_TOKENS";
    public static final String PROPERTY_URL_SERVERS = "_CONFIG_SERVER_%s_URL";

    public static final String PATH_TO_PROPS_CONFIG_KEY = "clusterConfig";

    private ConcurrentHashMap<Long, String> tokensToServers;
    private ConcurrentHashMap<String, Server> servers;
    private int bftReplicationFactor = -1;
    private int clusterConfigurationstamp = 1;

    public ClusterConfiguration() {
        tokensToServers = new ConcurrentHashMap<Long, String>(getShardTokens());
        servers = new ConcurrentHashMap<String, Server>();
        loadConfigurationFromFile();
    }

    public ClusterConfiguration(final ClusterConfiguration existingCC) {
        synchronized (existingCC) {
            this.bftReplicationFactor = existingCC.bftReplicationFactor;
            this.clusterConfigurationstamp = existingCC.clusterConfigurationstamp;
            this.tokensToServers = new ConcurrentHashMap<Long, String>(existingCC.tokensToServers);
            this.servers = new ConcurrentHashMap<String, Server>(existingCC.servers);
        }
    }

    public static boolean configurationExternal() {
        return System.getProperty(PATH_TO_PROPS_CONFIG_KEY) != null;
    }

    private void loadConfigurationFromFile() {
        final String pathToConfig = System.getProperty(PATH_TO_PROPS_CONFIG_KEY);
        if (pathToConfig == null) {
            LOG.info("No configuration specified");
            return;
        }
        LOG.info("Loading configuration from {}", pathToConfig);
        Utils.assertTrue(new File(pathToConfig).exists());
        Properties props = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(pathToConfig);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        loadInitialConfigurationFromProperties(props);
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

    public Set<Server> getAllServers() {
        final Set<Server> outServers = new HashSet<Server>();
        for (final Server s : servers.values()) {
            outServers.add(s);
        }
        return outServers;
    }

    public Set<String> getAllServerIds() {
        return servers.keySet();
    }

    public Map<String, Server> getSeverIdToServerMapping() {
        final Map<String, Server> outServers = new HashMap<String, Server>();
        for (final String serverId : servers.keySet()) {
            outServers.put(serverId, servers.get(serverId));
        }
        return outServers;
    }

    public void loadInitialConfigurationFromProperties(final Properties props) {
        LOG.debug("Loading initial configuration from properties {}", props);
        final String[] allServers = splitMultiple(props.getProperty(PROPERTY_SERVERS, ""));
        for (final String serverId : allServers) {
            // Getting server
            final String serverUrl = props.getProperty(String.format(PROPERTY_URL_SERVERS, serverId), null);
            if (serverUrl == null) {
                throw new IllegalStateException(String.format("Missing server url for id %s", serverId));
            }
            servers.put(serverId, new Server(serverUrl));
            
            // Getting token
            final String[] tokensForServer = splitMultiple(props.getProperty(String.format(PROPERTY_PREF_SERVERS, serverId),
                    ""));
            LOG.debug("{} tokens are given to server {}", tokensForServer.length, serverId);
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
                tokensToServers.put(token, serverId);

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
        {
            final String bftProperty = props.getProperty(PROPERTY_BFT_REPLICATION);
            if (bftProperty == null) {
                throw new IllegalArgumentException(String.format("BFT replication factor is non defined in %s",
                        props.stringPropertyNames()));
            }
            final int inBftReplicationFactor = Integer.parseInt(bftProperty);
            Utils.assertTrue(inBftReplicationFactor >= 4, "BFT replication factor should be > 4");
            Utils.assertTrue(inBftReplicationFactor <= tokensToServers.values().size(),
                    "BFT replication factor should be less or equal than number of servers");
            this.bftReplicationFactor = inBftReplicationFactor;
        }
    }

    public String getServerForObject(final String object) {
        final List<String> servers = getServersForObjectHashCode(object.hashCode(), bftReplicationFactor);
        return servers.get(0);
    }

    public List<String> getServersForObject(final String object) {
        return getServersForObjectHashCode(object.hashCode(), bftReplicationFactor);
    }
    
    public Set<Server> getServerSetStoringObject(final String object) {
        List<String> serverNames = getServersForObject(object);
        Set<Server> serverSet = new HashSet<Server>();
        for (String name : serverNames ) {
            serverSet.add(servers.get(name));
        }
        return serverSet;
    }

    public List<String> getServersForObjectHashCode(int hashCode, int bftReplicationFactor) {
        Utils.assertTrue(bftReplicationFactor >= 4,
                String.format("Replication factor for BFT should be >= 4. %s specified", bftReplicationFactor));
        final Long hashCodeUnsigned = intToUnsignedLong(hashCode);
        final Integer tokenValStart = (int) (hashCodeUnsigned / SHARD_TOKEN_VALUE_RANGE);
        final List<String> servers = new ArrayList<String>(bftReplicationFactor);
        for (int i = 0; i < bftReplicationFactor; i++) {
            final int ithTokenValue = (tokenValStart + i) % SHARD_TOKENS;
            final Long token = tokenNumberToTokenValue(i);
            final String server = getServerForToken(token);
            if (servers.contains(server)) {
                throw new IllegalStateException(
                        String.format(
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

    public int getClusterConfigurationstamp() {
        return clusterConfigurationstamp;
    }

    public int getReplicationFactor() {
        return bftReplicationFactor;
    }

    public int getServerMajority() {
        final int f = getReplicationFactor() / 3;
        return 2 * f + 1;
    }
}
