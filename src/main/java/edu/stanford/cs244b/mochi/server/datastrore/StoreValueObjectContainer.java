package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.GrantTimestamp;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ObjectCertificate;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;


public class StoreValueObjectContainer<T> {

    private volatile T value;
    private volatile boolean valueAvailble;
    private volatile ObjectCertificate currentC;
    private volatile GrantTimestamp grantTimestamp;

    // TODO: change to Write-1 ??
    private final List<Operation> ops = new ArrayList<Operation>(4);
    private final Map<String, OldOpsEntry> oldOps = new HashMap<String, OldOpsEntry>();
    private volatile long currentVS;

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

    public GrantTimestamp getGrantTimestamp() {
        return grantTimestamp;
    }

    public void setGrantTimestamp(GrantTimestamp grantTimestamp) {
        this.grantTimestamp = grantTimestamp;
    }

    public long getCurrentVS() {
        return currentVS;
    }

    public void setCurrentVS(long currentVS) {
        this.currentVS = currentVS;
    }

    public ObjectCertificate getCurrentC() {
        return currentC;
    }

    public void setCurrentC(ObjectCertificate currentC) {
        this.currentC = currentC;
    }
}
