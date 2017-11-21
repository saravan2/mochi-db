package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrantCertificateElement;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrantElement;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteGrant;

public class StoreValueObjectContainer<T> {

    private final String key;
    private volatile T value;
    private volatile boolean valueAvailble;
    private volatile WriteCertificate currentC;
    private volatile MultiGrantElement grantTimestamp;

    // TODO: change to Write-1 ??
    private final List<Operation> ops = new ArrayList<Operation>(4);
    private final Map<String, OldOpsEntry> oldOps = new HashMap<String, OldOpsEntry>();
    private volatile long currentVS = 1;
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

    public MultiGrantElement getGrantTimestamp() {
        return grantTimestamp;
    }

    public void setGrantTimestamp(MultiGrantElement grantTimestamp) {
        this.grantTimestamp = grantTimestamp;
    }

    public long getCurrentVS() {
        return currentVS;
    }

    public void setCurrentVS(long currentVS) {
        this.currentVS = currentVS;
    }

    public WriteCertificate getCurrentC() {
        return currentC;
    }

    public MultiGrantElement getMultiGrantElementFromWriteCertificate() {
        if (currentC == null) {
            return null;
        }
        final List<WriteGrant> writeGrants = currentC.getWriteGrantsList();
        if (writeGrants.size() == 0) {
            throw new IllegalStateException("Write certificate should contain at least one write grant");
        }
        // Note: Write grants should be identical
        final WriteGrant anyWriteGrant = writeGrants.get(0);
        final List<MultiGrantCertificateElement> listOfMultiGrantElements = anyWriteGrant.getMultiGrantOListList();
        if (listOfMultiGrantElements.size() == 0) {
            throw new IllegalStateException("listOfMultiGrantElements is 0");
        }
        for (final MultiGrantCertificateElement mgce : listOfMultiGrantElements) {
            final MultiGrantElement mge = mgce.getMultiGrantElement();
            if (mge == null) {
                throw new IllegalStateException(String.format(
                        "MultiGrantElement is empty for MultiGrantCertificateElement: %s", mgce));
            }
            if (mge.getObjectId().equals(this.key)) {
                return mge;
            }
        }
        return null;
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
