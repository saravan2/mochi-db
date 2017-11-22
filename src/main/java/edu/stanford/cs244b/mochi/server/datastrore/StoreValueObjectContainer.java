package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Grant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;

public class StoreValueObjectContainer<T> {
    public static final int VIEWSTAMP_START_NUMBER = 1;
    // Key to which that object belongs
    private final String key;
    // Value if any
    private volatile T value;
    // For removed and newly created objects `valueAvailble` is false
    private volatile boolean valueAvailble;
    // How current object happen to be the way it is.
    private volatile WriteCertificate currentC;
    // Grant on that object if any
    private volatile Grant grantTimestamp;

    // TODO: change to Write-1 ??
    private final List<Operation> ops = new ArrayList<Operation>(4);
    private final Map<String, OldOpsEntry> oldOps = new HashMap<String, OldOpsEntry>();
    private volatile long currentVS = VIEWSTAMP_START_NUMBER;
    private final ReentrantLock objectLock = new ReentrantLock();

    public StoreValueObjectContainer(String key, boolean valueAvailble) {
        this(key, null, valueAvailble);
    }

    public StoreValueObjectContainer(String key, T value, boolean valueAvailble) {
        this.value = value;
        this.valueAvailble = valueAvailble;
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public boolean isValueAvailble() {
        return valueAvailble;
    }

    public void setValueAvailble(boolean valueAvailble) {
        this.valueAvailble = valueAvailble;
    }

    public Long getOperationNumberInOldOps(final String clientId) {
        final OldOpsEntry clientOldOpsEntry = oldOps.get(clientId);
        if (clientOldOpsEntry == null) {
            return null;
        }
        return clientOldOpsEntry.getOperationNumber();
    }

    public boolean isOperationInOps(final Operation op) {
        for (final Operation opFromOps : ops) {
            if (opFromOps.getOperationNumber() == op.getOperationNumber()) {
                return true;
            }
        }
        return false;
    }

    public void addOperationToOps(final Operation op) {
        this.ops.add(op);
    }

    public Grant getGrantTimestamp() {
        return grantTimestamp;
    }

    public void setGrantTimestamp(Grant grantTimestamp) {
        this.grantTimestamp = grantTimestamp;
    }

    public long getCurrentVS() {
        return currentVS;
    }

    public void setCurrentVS(long currentVS) {
        this.currentVS = currentVS;
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

    public void acquireObjectLockIfNotHeld() {
        if (objectLock.isHeldByCurrentThread() == false) {
            objectLock.lock();
        }
    }

    public void releaseObjectLock() {
        objectLock.unlock();
    }

    public void releaseObjectLockIfHeldByCurrent() {
        if (objectLock.isHeldByCurrentThread()) {
            objectLock.unlock();
        }
    }
}
