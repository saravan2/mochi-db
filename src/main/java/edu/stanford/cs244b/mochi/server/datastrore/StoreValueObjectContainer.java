package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Grant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;

public class StoreValueObjectContainer<T> {
    final static Logger LOG = LoggerFactory.getLogger(StoreValueObjectContainer.class);

    public static final int EPOCH_START = 0;
    // Key to which that object belongs
    private final String key;
    // Value if any
    private volatile T value;
    // For removed and newly created objects `valueAvailble` is false
    private volatile boolean valueAvailble;
    // How current object happen to be the way it is.
    private volatile WriteCertificate currentC;

    // Note: working on givenWrite1Grants requires acquiring object lock
    private final HashMap<Long, HashMap<Long, Pair<String, Grant>>> givenWrite1Grants;

    private volatile long currentEpoch = EPOCH_START;
    private final ReentrantReadWriteLock objectLock = new ReentrantReadWriteLock();

    public StoreValueObjectContainer(String key, boolean valueAvailble) {
        this(key, null, valueAvailble);
    }

    public StoreValueObjectContainer(String key, T value, boolean valueAvailble) {
        this.value = value;
        this.valueAvailble = valueAvailble;
        this.key = key;
        givenWrite1Grants = new HashMap<Long, HashMap<Long, Pair<String, Grant>>>();

    }
    
    public String getKey() {
        return this.key;
    }

    public T getValue() {
        // TODO: assert that lock is held before operation with values
        return value;
    }

    public T setValue(T value) {
        // TODO: assert that lock is held before operation with values
        final T oldValue = this.value;
        this.value = value;
        return oldValue;
    }

    public boolean isValueAvailble() {
        return valueAvailble;
    }
    
    public long getCurrentEpoch() {
        return currentEpoch;
    }
    
    public void setCurrentEpoch(long currentEpoch) {
        this.currentEpoch = currentEpoch;
    }
    
    public void moveToNextEpochIfNecessary(long timestamp) {
        long nextEpoch = (((timestamp/1000) + 1)*1000);
        if (currentEpoch < nextEpoch) {
            currentEpoch = nextEpoch;
        }
    }


    public boolean setValueAvailble(boolean valueAvailble) {
        final boolean oldAvailable = this.valueAvailble;
        this.valueAvailble = valueAvailble;
        return oldAvailable;
    }
   
    public HashMap<Long, HashMap<Long, Pair<String, Grant>>> getGivenWrite1Grants() {
        return givenWrite1Grants;
    }
    
    public final Long mapTimeStampsToEpoch(Long timestamp) {
        return ((timestamp/1000) * 1000);
    }
    
    public void addGivenWrite1Grant(Long timestamp, Grant grant) {
        final Long epoch = mapTimeStampsToEpoch(timestamp);
        final Pair<String, Grant> hashedGrant = new Pair<String, Grant>(grant.getTransactionHash(), grant);
        if (givenWrite1Grants.containsKey(epoch)) {
            givenWrite1Grants.get(epoch).put(timestamp, hashedGrant);
        } else {
            HashMap<Long, Pair<String, Grant>> entry = new HashMap<Long, Pair<String, Grant>>();
            entry.put(timestamp, hashedGrant);
            givenWrite1Grants.put(epoch, entry);
        }
    }
    
    public final HashMap<Long, Pair<String, Grant>> getAllGrantsFromEpoch(Long timestamp) {
        final Long epoch = mapTimeStampsToEpoch(timestamp);
        if (givenWrite1Grants.containsKey(epoch)) {
            return givenWrite1Grants.get(epoch);
        }
        return null;
    }
    
    public Grant getGrantIfExistsAtTimeStamp(Long timestamp) {
        final Long epoch = mapTimeStampsToEpoch(timestamp);
        if (givenWrite1Grants.containsKey(epoch)) {
            HashMap<Long, Pair<String, Grant>> grantTimeStamps = givenWrite1Grants.get(epoch);
            if (grantTimeStamps.containsKey(timestamp)) {
                return grantTimeStamps.get(timestamp).getValue1();
            }
        }
        return null;
    }
    
    public boolean grantAlreadyGiven(Long timestamp) {
        Long epoch = mapTimeStampsToEpoch(timestamp);
        if (givenWrite1Grants.containsKey(epoch)) {
            HashMap<Long, Pair<String, Grant>> grantTimeStamps = givenWrite1Grants.get(epoch);
            if (grantTimeStamps.containsKey(timestamp)) {
                return true;
            }
        }
        return false;
    }
    
    public void deleteGivenWrite1Grant(Long timestamp) {
        final Long epoch = mapTimeStampsToEpoch(timestamp);
        if (givenWrite1Grants.containsKey(epoch)) {
            if (givenWrite1Grants.get(epoch).containsKey(timestamp)) {
                givenWrite1Grants.get(epoch).remove(timestamp);
                if (givenWrite1Grants.get(epoch).isEmpty()) {
                    givenWrite1Grants.remove(epoch);
                }
            }
        }
    }
    
    public void truncateGivenWrite1Grants() {
        if (currentEpoch >= 3000) {
            Set<Long> epochs = givenWrite1Grants.keySet();
            for (Long epoch : epochs) {
                if ((currentEpoch > epoch) && ((currentEpoch-epoch) >= 2000)) {
                    givenWrite1Grants.get(epoch).clear();
                    givenWrite1Grants.remove(epoch);
                }
            
            }
        }
    }
    
    public boolean isGivenWrite1GrantsEmpty() {
        return givenWrite1Grants.isEmpty();
    }

    public Long getCurrentTimestampFromCurrentCertificate() {
        if (currentC == null) {
            return null;
        }
        final Map<String, MultiGrant> mutliGrantsMap = currentC.getGrantsMap();
        Utils.assertNotNull(mutliGrantsMap, "MultiGrants map should not be null");
        Long timestamp = null;
        for (final MultiGrant multiGrant : mutliGrantsMap.values()) {
            final Map<String, Grant> allGrants = multiGrant.getGrantsMap();
            Utils.assertNotNull(allGrants, "Grants map should not be null");
            final Grant grantForCurrentKey = allGrants.get(key);
            Utils.assertNotNull(grantForCurrentKey, "grantForCurrentKey map should not be null");
            long timestampFromGrant = grantForCurrentKey.getTimestamp();
            if (timestamp == null) {
                timestamp = timestampFromGrant;
            } else {
                // Checking that is exactly the same
                if (timestamp != timestampFromGrant) {
                    throw new IllegalStateException("Timestamp mismatch between grants from different servers");
                }
            }
        }
        return timestamp;
    }

    public WriteCertificate getCurrentC() {
        return currentC;
    }

    public Grant getMultiGrantElementFromWriteCertificate() {
        if (currentC == null) {
            // if object was just created
            return null;
        }
        final Map<String, MultiGrant> writeGrants = currentC.getGrantsMap();
        if (writeGrants.size() == 0) {
            throw new IllegalStateException("Write certificate should contain at least one write grant");
        }
        // Note: Write grants should be identical
        final Collection<MultiGrant> allGrants = writeGrants.values();

        MultiGrant anyWriteGrant = null;
        for (MultiGrant mg : allGrants) {
            anyWriteGrant = mg;
            break;
        }
        final Grant grant = anyWriteGrant.getGrantsMap().get(this.key);
        return grant;
    }

    public void setCurrentC(WriteCertificate currentC) {
        this.currentC = currentC;
    }

    public void acquireObjectWriteLockIfNotHeld() {
        if (objectLock.isWriteLockedByCurrentThread() == false) {
            objectLock.writeLock().lock();
        }
    }
    
    public void acquireObjectReadLock() {
        objectLock.readLock().lock();
    }

    public void releaseObjectWriteLockIfHeldByCurrent() {
        if (objectLock.isWriteLockedByCurrentThread()) {
            objectLock.writeLock().unlock();
        }
    }
    
    public void releaseObjectReadLock() {
        objectLock.readLock().unlock();
    }

    public boolean isObjectWriteLockHeldByCurrent() {
        return objectLock.isWriteLockedByCurrentThread();
    }
}
