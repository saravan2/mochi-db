package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Grant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
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

public class InMemoryDataStore implements DataStore {
    private final static Logger LOG = LoggerFactory.getLogger(InMemoryDataStore.class);

    private final ConcurrentHashMap<String, StoreValueObjectContainer<String>> data = new ConcurrentHashMap<String, StoreValueObjectContainer<String>>();
    private final MochiContext mochiContext;

    @SuppressWarnings("unchecked")
    public InMemoryDataStore(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
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

    protected Triplet<Grant, WriteCertificate, Boolean> processWrite(final Operation op, final String clientId) {
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

                final Grant.Builder grantBuilder = Grant.newBuilder();
                grantBuilder.setObjectId(interestedKey);
                grantBuilder.setOperationNumber(op.getOperationNumber());
                grantBuilder.setViewstamp(storeValue.getCurrentVS());

                Long timestampFromCertificate = storeValue.getCurrentTimestampFromCurrentCertificate();
                if (timestampFromCertificate != null) {
                    grantBuilder.setTimestamp(timestampFromCertificate);
                }
                final Grant newGrant = grantBuilder.build();

                storeValue.setGrantTimestamp(grantBuilder.build());

                return Triplet.with(newGrant, storeValue.getCurrentC(), true);
            } else {
                /* Somebody else has the grant. Write refuse */
                return Triplet.with(storeValue.getGrantTimestamp(), storeValue.getCurrentC(), false);
            }
        }
    }

    protected StoreValueObjectContainer getOrCreateStoreValue(final String interestedKey) {
        final StoreValueObjectContainer<String> valueContainer;
        if (data.contains(interestedKey)) {
            valueContainer = data.get(interestedKey);
        } else {
            final StoreValueObjectContainer<String> possibleStoreValueContainerForThatKey = new StoreValueObjectContainer<String>(
                    interestedKey,
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
        final Map<String, WriteCertificate> currentObjectCertificates = new HashMap<String, WriteCertificate>(
                operations.size());
        final Map<String, Grant> grants = new HashMap<String, Grant>();
        
        boolean allWriteOk = true;
        for (Operation op : operations) {
            if (op.getAction() == OperationAction.WRITE) {
                Triplet<Grant, WriteCertificate, Boolean> wrteResult = processWrite(op, write1ToServer.getClientId());
                final Grant grant = wrteResult.getValue0();
                Utils.assertNotNull(grant, "Grant cannot be null");
                final WriteCertificate writeCertificate = wrteResult.getValue1();
                final boolean grantWasGranted = wrteResult.getValue2();
                
                final String objectId = grant.getObjectId();
                Utils.assertNotNull(objectId, "objectId cannot be null");
                currentObjectCertificates.put(objectId, writeCertificate);
                grants.put(objectId, grant);
                
                if (grantWasGranted == false) {
                    LOG.debug("GrantWasNot Granted for opeation {}. Grant = {}", op, grant);
                    allWriteOk = false;
                }
            }

        }

        if (allWriteOk) {
            final MultiGrant.Builder mgb = MultiGrant.newBuilder();
            mgb.setClientId(write1ToServer.getClientId());
            mgb.setServerId(mochiContext.getServerId());
            mgb.putAllGrants(grants);
            
            final Write1OkFromServer.Builder builder = Write1OkFromServer.newBuilder();
            builder.setMultiGrant(mgb);
            builder.putAllCurrentCertificates(currentObjectCertificates);
            return builder.build();
        }
        throw new UnsupportedOperationException();
    }

    private List<String> getObjectsToLock(final WriteGrant writeGrant) {
        final List<MultiGrantCertificateElement> multiGrantElements = writeGrant.getMultiGrantOListList();
        final List<String> objectsToLock = new ArrayList<String>(multiGrantElements.size());
        for (MultiGrantCertificateElement certificateElement : multiGrantElements) {
            final Grant multiGrantElement = certificateElement.getMultiGrantElement();
            objectsToLock.add(multiGrantElement.getObjectId());
        }
        // Need to do in order to avoid deadlock
        Collections.sort(objectsToLock);
        return objectsToLock;
    }

    private Grant getMultiGrantElementFromWriteGrant(final WriteGrant writeGrant,
            final String interestedObjectId) {
        final List<MultiGrantCertificateElement> multiGrantElements = writeGrant.getMultiGrantOListList();
        for (MultiGrantCertificateElement certificateElement : multiGrantElements) {
            final Grant multiGrantElement = certificateElement.getMultiGrantElement();
            if (interestedObjectId.equals(multiGrantElement.getObjectId())) {
                return multiGrantElement;
            }
        }
        return null;
    }

    /*
     * Attempt to acquire locks on object which are referenced by write grants,
     * after that we check for timestamp and viewstamp to identify whether there
     * are new changes that needs to be pulled from other replicas. Also this
     * method validates whether write grants are the same. If not, it throws an
     * exception. That is a responsibiloty of the client to make sure that
     * identical grants are passed in phase 2
     */
    private List<String> write2acquireLocksAndCheckViewStamps(List<WriteGrant> writeGrants) {
        if (writeGrants.size() == 0) {
            throw new IllegalStateException("There should be at least one grant");
        }
        final WriteGrant writeGrant = writeGrants.get(0);

        final List<String> objectsToLock = getObjectsToLock(writeGrant);
        for (String object : objectsToLock) {
            final StoreValueObjectContainer<String> storeValueContianer = data.get(object);
            storeValueContianer.acquireObjectLockIfNotHeld();
        }
        LOG.debug("All locks for write grant acquired: {}", objectsToLock);
        final List<String> listOfObjectsWhoseTimestampIsOld = new ArrayList<String>();
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
            final Long objectCertificateTS;
            final Long objectCertificateVS;
            if (objectCertificateGrant != null) {
                final Grant mge = getMultiGrantElementFromWriteGrant(objectCertificateGrant, object);
                objectCertificateTS = mge.getTimestamp();
                objectCertificateVS = mge.getViewstamp();
            } else {
                objectCertificateTS = null;
                objectCertificateVS = null;
            }
            final Grant mgeFromWriteGrant = getMultiGrantElementFromWriteGrant(writeGrant, object);
            if (mgeFromWriteGrant == null) {
                throw new IllegalStateException(String.format("Failed to find MultiGrantElement for '%s' in '%s'",
                        object, writeGrant));
            }
            final long grantVS = mgeFromWriteGrant.getViewstamp();
            final long grantTS = mgeFromWriteGrant.getTimestamp();
            LOG.debug(
                    "Got grants and certificate VS and TS in write2acquireLocksAndCheckViewStamps. grant = [TS {}, VS {}], object = [TS {}, VS {}]",
                    grantTS, grantVS, objectCertificateTS, objectCertificateVS);

            if (objectCertificateTS == null && objectCertificateVS == null) {
                if (storeValueContianer.isValueAvailble()) {
                    throw new IllegalStateException(
                            "objectCertificateTS and objectCertificateVS are null but object value is availble");
                }
                continue;
            }
            if (objectCertificateVS == grantVS && objectCertificateTS == grantTS - 1) {
                LOG.debug("Timecheck for object {} is successful", object);
                continue;
            }
            listOfObjectsWhoseTimestampIsOld.add(object);
        }
        LOG.debug("Timestamps checked. listOfObjectsWhoseTimestampIsOld = {}", listOfObjectsWhoseTimestampIsOld);

        // TODO: Check writeGrants are the same

        return listOfObjectsWhoseTimestampIsOld;
    }

    private void write2releaseLocks(final WriteGrant writeGrant) {
        final List<String> objectsToLock = getObjectsToLock(writeGrant);
        for (String object : objectsToLock) {
            final StoreValueObjectContainer<String> storeValueContianer = data.get(object);
            storeValueContianer.releaseObjectLockIfHeldByCurrent();
        }
    }

    private void write2apply() {

    }

    @Override
    public Object processWrite2ToServer(Write2ToServer write2ToServer) {

        final WriteCertificate wc = write2ToServer.getWriteCertificate();
        // TODO: check for oldOps for duplicates

        final List<WriteGrant> writeGrants = wc.getWriteGrantsList();
        try {
            final List<String> listOfObjectsWhoseTimestampIsOld = write2acquireLocksAndCheckViewStamps(writeGrants);
            if (listOfObjectsWhoseTimestampIsOld.size() != 0) {
                // TODO: do data loading from remotes
                throw new UnsupportedOperationException();
            }
            LOG.debug("Timestmaps were checked, locks are held and it's time to apply the operation");
            write2apply();

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
