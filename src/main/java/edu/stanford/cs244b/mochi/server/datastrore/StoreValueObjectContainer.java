package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrantElement;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;

public class StoreValueObjectContainer<T> {

    private volatile T value;
    private volatile boolean valueAvailble;
    private volatile WriteCertificate currentC;
    private volatile MultiGrantElement grantTimestamp;

    // TODO: change to Write-1 ??
    private final List<Operation> ops = new ArrayList<Operation>(4);
    private final Map<String, OldOpsEntry> oldOps = new HashMap<String, OldOpsEntry>();
    private volatile long currentVS = 1;
    private final ReentrantLock objectLock = new ReentrantLock();

    public StoreValueObjectContainer(boolean valueAvailble) {
        this(null, valueAvailble);
    }

    public StoreValueObjectContainer(T value, boolean valueAvailble) {
        this.value = value;
        this.valueAvailble = valueAvailble;
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
