package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrantCertificateElement;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrantElement;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationAction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1OkFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2AnsFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteGrant;

public class InMemoryDataStore implements DataStore {
    private final static Logger LOG = LoggerFactory.getLogger(InMemoryDataStore.class);

    private final ConcurrentHashMap<String, StoreValueObjectContainer<String>> data = new ConcurrentHashMap<String, StoreValueObjectContainer<String>>();

    @SuppressWarnings("unchecked")
    public InMemoryDataStore() {
        // TODO: remove that later on
        data.put("DEMO_KEY_1", new StoreValueObjectContainer<String>("value1", true));
        data.put("DEMO_KEY_2", new StoreValueObjectContainer<String>("value2", true));
        data.put("DEMO_KEY_3", new StoreValueObjectContainer<String>("value3", true));
    }

    protected OperationResult processRead(final Operation op) {
        final String interestedKey = op.getOperand1();
        checkOp1IsNonEmptyKeyError(interestedKey);
        final StoreValueObjectContainer<String> keyStoreValue = data.get(interestedKey);
        final String valueForTheKey;
        if (keyStoreValue == null) {
            valueForTheKey = null;
        } else {
            valueForTheKey = keyStoreValue.getValue();
        }
        final OperationResult.Builder operationResultBuilder = OperationResult.newBuilder();
        operationResultBuilder.setResult(valueForTheKey);
        return operationResultBuilder.build();
    }

    protected Pair<MultiGrantCertificateElement, Boolean> processWrite(final Operation op, final String clientId) {
        final String interestedKey = op.getOperand1();
        checkOp1IsNonEmptyKeyError(interestedKey);
        LOG.debug("Performing processWrite on key: {}", interestedKey);
        final StoreValueObjectContainer storeValue = getOrCreateStoreValue(interestedKey);
        synchronized (storeValue) {
            final Long oprationNumberInOldOps = storeValue.getOperationNumberInOldOps(clientId);
            if (oprationNumberInOldOps != null && oprationNumberInOldOps < op.getOperationNumber()) {
                throw new TooOldRequestException();
            }
            if (oprationNumberInOldOps != null && oprationNumberInOldOps == op.getOperationNumber()) {
                // TODO: reply old
                throw new UnsupportedOperationException();
            }
            if (storeValue.isOperationInOps(op)) {
                // TODO: reply previous value
                throw new UnsupportedOperationException();
            }
            storeValue.addOperationToOps(op);
            if (storeValue.getGrantTimestamp() == null) {
                /* There is no current grant on that timestamp */

                final MultiGrantElement.Builder builder = MultiGrantElement.newBuilder();
                builder.setObjectId(interestedKey);
                builder.setOperationNumber(op.getOperationNumber());
                builder.setViewstamp(storeValue.getCurrentVS());

                storeValue.setGrantTimestamp(builder.build());

                final MultiGrantCertificateElement.Builder mgceBuilder = MultiGrantCertificateElement.newBuilder();
                mgceBuilder.setMultiGrantElement(storeValue.getGrantTimestamp());

                if (storeValue.getCurrentC() != null) {
                    mgceBuilder.setCurrentC(storeValue.getCurrentC());
                }

                return Pair.with(mgceBuilder.build(), true);
            } else {
                /* Somebody else has the grant. Write refuse */
                final MultiGrantCertificateElement.Builder mgceBuilder = MultiGrantCertificateElement.newBuilder();
                mgceBuilder.setMultiGrantElement(storeValue.getGrantTimestamp());

                if (storeValue.getCurrentC() != null) {
                    mgceBuilder.setCurrentC(storeValue.getCurrentC());
                }

                return Pair.with(mgceBuilder.build(), false);
            }
        }
    }

    protected StoreValueObjectContainer getOrCreateStoreValue(final String interestedKey) {
        final StoreValueObjectContainer<String> valueContainer;
        if (data.contains(interestedKey)) {
            valueContainer = data.get(interestedKey);
        } else {
            final StoreValueObjectContainer<String> possibleStoreValueContainerForThatKey = new StoreValueObjectContainer<String>(
                    false);
            final StoreValueObjectContainer<String> containerIfWasCreated = data.putIfAbsent(interestedKey,
                    possibleStoreValueContainerForThatKey);
            if (containerIfWasCreated == null) {
                valueContainer = possibleStoreValueContainerForThatKey;
            } else {
                valueContainer = containerIfWasCreated;
            }
        }
        assert valueContainer != null;
        return valueContainer;
    }

    protected void checkAndFailOnReadOnly(final boolean readOnly) {
        if (readOnly) {
            throw new IllegalStateException("Attempt to execute non read only transactioin with readOnly flag");
        }
    }

    protected void checkOp1IsNonEmptyKeyError(final String key) {
        if (StringUtils.isEmpty(key)) {
            throw new IllegalArgumentException("Specified key is null or empty in operand 1");
        }
    }

    @Override
    public Object processReadRequest(ReadToServer readToServer) {
        final Transaction transaction = readToServer.getTransaction();
        LOG.debug("Processing readToServer with transaction: {}", transaction);
        final List<Operation> operations = transaction.getOperationsList();
        if (operations == null) {
            return null;
        }
        final List<OperationResult> operationResults = new ArrayList<OperationResult>(operations.size());
        for (Operation op : operations) {
            if (op.getAction() == OperationAction.READ) {
                operationResults.add(processRead(op));
            } else {
                checkAndFailOnReadOnly(true);
            }

        }
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(operationResults);
        return transactionResultBuilder.build();
    }

    @Override
    public Object processWrite1ToServer(Write1ToServer write1ToServer) {
        final Transaction transaction = write1ToServer.getTransaction();
        LOG.debug("Executing  write transaction: {}", transaction);
        final List<Operation> operations = transaction.getOperationsList();
        if (operations == null) {
            return null;
        }
        final List<MultiGrantCertificateElement> mgceList = new ArrayList<MultiGrantCertificateElement>(
                operations.size());
        boolean allWriteOk = true;
        for (Operation op : operations) {
            if (op.getAction() == OperationAction.WRITE) {
                Pair<MultiGrantCertificateElement, Boolean> wrteResult = processWrite(op, write1ToServer.getClientId());
                final MultiGrantCertificateElement mgce = wrteResult.getValue0();
                final boolean grantWasGranted = wrteResult.getValue1();
                mgceList.add(mgce);
                if (grantWasGranted == false) {
                    LOG.debug("GrantWasNot Granted for opeation {}. MultiGrantCertificateElement = {}", op, mgce);
                    allWriteOk = false;
                }
            }

        }

        if (allWriteOk) {
            final Write1OkFromServer.Builder builder = Write1OkFromServer.newBuilder();
            final WriteGrant.Builder writeGrantBuilder = WriteGrant.newBuilder();
            writeGrantBuilder.addAllMultiGrantOList(mgceList);
            builder.setWriteGrant(writeGrantBuilder);
            return builder.build();
        }
        throw new UnsupportedOperationException();
    }

    private List<String> getObjectsToLock(final WriteGrant writeGrant) {
        final List<MultiGrantCertificateElement> multiGrantElements = writeGrant.getMultiGrantOListList();
        final List<String> objectsToLock = new ArrayList<String>(multiGrantElements.size());
        for (MultiGrantCertificateElement certificateElement : multiGrantElements) {
            final MultiGrantElement multiGrantElement = certificateElement.getMultiGrantElement();
            objectsToLock.add(multiGrantElement.getObjectId());
        }
        // Need to do in order to avoid deadlock
        Collections.sort(objectsToLock);
        return objectsToLock;
    }

    private MultiGrantElement getMultiGrantElementFromWriteCertificateForWriteGrant(final WriteGrant writeGrant,
            final String interestedObjectId) {
        final List<MultiGrantCertificateElement> multiGrantElements = writeGrant.getMultiGrantOListList();
        for (MultiGrantCertificateElement certificateElement : multiGrantElements) {
            final MultiGrantElement multiGrantElement = certificateElement.getMultiGrantElement();
            if (interestedObjectId.equals(multiGrantElement.getObjectId())) {
                return multiGrantElement;
            }
        }
        return null;
    }

    private void write2acquireLocksAndCheckViewStamps(final WriteGrant writeGrant) {
        final List<String> objectsToLock = getObjectsToLock(writeGrant);
        for (String object : objectsToLock) {
            final StoreValueObjectContainer<String> storeValueContianer = data.get(object);
            storeValueContianer.acquireObjectLockIfNotHeld();
        }
        LOG.debug("All locks for write grant acquired: {}", objectsToLock);
        for (String object : objectsToLock) {
            final StoreValueObjectContainer<String> storeValueContianer = data.get(object);
            final long objectViewStamp = storeValueContianer.getCurrentVS();
            final WriteCertificate objectWC = storeValueContianer.getCurrentC();
            final WriteGrant objectCertificateGrant;
            if (objectWC != null) {
                objectCertificateGrant = objectWC.getWriteGrants(0);
            } else {
                objectCertificateGrant = null;
            }
            final long objectCertificateTimestamp;
            if (objectCertificateGrant != null) {
                final MultiGrantElement mge = getMultiGrantElementFromWriteCertificateForWriteGrant(objectCertificateGrant, object);
                objectCertificateTimestamp = mge.getTimestamp();
            }
            final MultiGrantElement mgeFromWriteGrant = getMultiGrantElementFromWriteCertificateForWriteGrant(writeGrant, object);
            if (mgeFromWriteGrant == null) {
                throw new IllegalStateException(String.format("Failed to find MultiGrantElement for '%s' in '%s'",
                        object, writeGrant));
            }
            final long grantVS = mgeFromWriteGrant.getViewstamp();
            final long grantTS = mgeFromWriteGrant.getTimestamp();
            // TODO: check timestamps
        }
        LOG.debug("Timestamps checked");

    }

    private void write2releaseLocks(final WriteGrant writeGrant) {
        final List<String> objectsToLock = getObjectsToLock(writeGrant);
        for (String object : objectsToLock) {
            final StoreValueObjectContainer<String> storeValueContianer = data.get(object);
            storeValueContianer.releaseObjectLockIfHeldByCurrent();
        }
    }

    @Override
    public Object processWrite2ToServer(Write2ToServer write2ToServer) {

        final WriteCertificate wc = write2ToServer.getWriteCertificate();
        // TODO: check for oldOps for duplicates

        final List<WriteGrant> writeGrants = wc.getWriteGrantsList();
        // TODO: Check writeGrants are the same
        try {
            write2acquireLocksAndCheckViewStamps(writeGrants.get(0));
        } catch (Exception ex) {
            LOG.error("Exception at write2acquireLocksAndCheckViewStamps:", ex);
            throw ex;
        } finally {
            write2releaseLocks(writeGrants.get(0));
        }

        final Write2AnsFromServer.Builder write2AnsFromServerBuilder = Write2AnsFromServer.newBuilder();
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();

        // TODO: execute business logic

        write2AnsFromServerBuilder.setResult(transactionResultBuilder);
        return write2AnsFromServerBuilder.build();
    }

}
