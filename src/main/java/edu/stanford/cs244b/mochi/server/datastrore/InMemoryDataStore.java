package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.google.protobuf.TextFormat;

import edu.stanford.cs244b.mochi.server.ClusterConfiguration;
import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.FailureMessageType;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Grant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationAction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.RequestFailedFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1OkFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1RefusedFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2AnsFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.WriteCertificate;

public class InMemoryDataStore implements DataStore {
    private final static Logger LOG = LoggerFactory.getLogger(InMemoryDataStore.class);
    private final static String CONFIG_KEY_PREFIX = "_CONFIG_";
    private final ConcurrentHashMap<String, StoreValueObjectContainer<String>> data = new ConcurrentHashMap<String, StoreValueObjectContainer<String>>();
    private final ConcurrentHashMap<String, StoreValueObjectContainer<String>> dataConfig = new ConcurrentHashMap<String, StoreValueObjectContainer<String>>();
    private final ClusterConfiguration clusterConfiguration;
    private final MochiContext mochiContext;

    @SuppressWarnings("unchecked")
    public InMemoryDataStore(final MochiContext mochiContext) {
        this.mochiContext = mochiContext;
        this.clusterConfiguration = mochiContext.getClusterConfiguration();
    }

    protected ConcurrentHashMap<String, StoreValueObjectContainer<String>> getDataMap(final String key) {
        if (key.startsWith(CONFIG_KEY_PREFIX)) {
            return dataConfig;
        } else {
            return data;
        }
    }

    protected OperationResult processRead(final Operation op) {
        final String interestedKey = op.getOperand1();
        checkOp1IsNonEmptyKeyError(interestedKey);
        LOG.debug("Performing processRead on key: {}", interestedKey);
        final StoreValueObjectContainer<String> keyStoreValue = getDataMap(interestedKey).get(interestedKey);
        final String valueForTheKey;
        final WriteCertificate currentC;
        if (keyStoreValue == null) {
            valueForTheKey = null;
            currentC = WriteCertificate.getDefaultInstance();
        } else {
            valueForTheKey = keyStoreValue.getValue();
            currentC = keyStoreValue.getCurrentC();
        }
        final OperationResult.Builder operationResultBuilder = OperationResult.newBuilder();
        operationResultBuilder.setResult(valueForTheKey);
        operationResultBuilder.setCurrentCertificate(currentC);
        return operationResultBuilder.build();
    }

    protected Triplet<Grant, WriteCertificate, Boolean> processWrite(final Operation op, final String clientId,
            final Write1ToServer writeToServer) {
        final String interestedKey = op.getOperand1();
        final Grant grantAtTS;
        checkOp1IsNonEmptyKeyError(interestedKey);
        LOG.debug("Performing processWrite on key: {}", interestedKey);
        final StoreValueObjectContainer<String> storeValue;
        if (op.getAction() == OperationAction.WRITE) {
            storeValue  = getOrCreateStoreValue(interestedKey);
        } else {
            storeValue = getStoreValue(interestedKey);
        } 
        synchronized (storeValue) {
            if (storeValue != null) {
                long prospectiveTS = storeValue.getCurrentEpoch() + writeToServer.getSeed();
                if (storeValue.isGivenWrite1GrantsEmpty() || ((grantAtTS = storeValue.getGrantIfExistsAtTimeStamp(prospectiveTS)) == null)) {
                    /* There is no current grant on that timestamp */
    
                    final Grant.Builder grantBuilder = Grant.newBuilder();
                    grantBuilder.setObjectId(interestedKey);
                    grantBuilder.setTransactionHash(writeToServer.getTransactionHash());
    
                    grantBuilder.setTimestamp(prospectiveTS);
                    LOG.debug("Grant awarded in epoch {} ts {} for key {}", storeValue.getCurrentEpoch(), prospectiveTS, interestedKey);
                    final Grant newGrant = grantBuilder.build();
    
                    storeValue.addGivenWrite1Grant(prospectiveTS, grantBuilder.build());
                    return Triplet.with(newGrant, storeValue.getCurrentC(), true);
                } else {
                    /* Could be a retry */
                    if (writeToServer.getTransactionHash().equals(grantAtTS.getTransactionHash())) {
                        return Triplet.with(grantAtTS, storeValue.getCurrentC(), true);
                    } else {
                        /* Someone else has the grant */
                        return Triplet.with(grantAtTS, storeValue.getCurrentC(), false);
                    }
                }
            } else {
                /* Delete write1 request failed */
                return Triplet.with(null, null, false);
            }
        } 
    }

    protected StoreValueObjectContainer<String> getOrCreateStoreValue(final String interestedKey) {
        final StoreValueObjectContainer<String> valueContainer;
        final ConcurrentHashMap<String, StoreValueObjectContainer<String>> dataMap = getDataMap(interestedKey);
        if (dataMap.contains(interestedKey)) {
            valueContainer = dataMap.get(interestedKey);
        } else {
            final StoreValueObjectContainer<String> possibleStoreValueContainerForThatKey = new StoreValueObjectContainer<String>(
                    interestedKey, false);
            final StoreValueObjectContainer<String> containerIfWasCreated = dataMap.putIfAbsent(interestedKey,
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
    
    protected StoreValueObjectContainer<String> getStoreValue(final String interestedKey) {
        final StoreValueObjectContainer<String> valueContainer;
        final ConcurrentHashMap<String, StoreValueObjectContainer<String>> dataMap = getDataMap(interestedKey);
        if (dataMap.contains(interestedKey)) {
            valueContainer = dataMap.get(interestedKey);
        } else {
            valueContainer = null;
        }
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
        
        final List<String> objectsToReadLock = acquireReadLocksAndReturnList(transaction);
        
        final List<OperationResult> operationResults = new ArrayList<OperationResult>(operations.size());
        
        for (Operation op : operations) {
            if (op.getAction() == OperationAction.READ) {
                operationResults.add(processRead(op));
            } else {
                checkAndFailOnReadOnly(true);
            }

        }
        
        releaseReadLocks(objectsToReadLock);
        
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(operationResults);
        final ReadFromServer.Builder readFromServerBuilder = ReadFromServer.newBuilder();
        readFromServerBuilder.setResult(transactionResultBuilder.build());
        readFromServerBuilder.setNonce(readToServer.getNonce());
        readFromServerBuilder.setRid(Utils.getUUID());
        return readFromServerBuilder.build(); 
    }

    public Object tryProcessWriteRegularly(final Write1ToServer write1ToServer) {
        final Transaction transaction = write1ToServer.getTransaction();
        LOG.debug("Executing  write transaction: {}", transaction);
        final List<Operation> operations = transaction.getOperationsList();
        if (operations == null) {
            return null;
        }
        final Map<String, WriteCertificate> currentObjectCertificates = new HashMap<String, WriteCertificate>(
                operations.size());
        final Map<String, WriteCertificate> currentRejectedObjectCertificates = new HashMap<String, WriteCertificate>(
                operations.size());
        final Map<String, Grant> grants = new HashMap<String, Grant>();
        final Map<String, Grant> rejectedGrants = new HashMap<String, Grant>();
        boolean allWriteOk = true;
        for (Operation op : operations) {
            if ((op.getAction() == OperationAction.WRITE) ||
                (op.getAction() == OperationAction.DELETE)) {
                Triplet<Grant, WriteCertificate, Boolean> wrteResult = processWrite(op, write1ToServer.getClientId(),
                        write1ToServer);
                final Grant grant = wrteResult.getValue0();
                final String objectId;
                if (grant != null) {
                    objectId = grant.getObjectId();
                } else {
                    objectId = op.getOperand1();
                }
                final WriteCertificate writeCertificate = wrteResult.getValue1();
                final boolean grantWasGranted = wrteResult.getValue2();
                Utils.assertNotNull(objectId, "objectId cannot be null");

                if (grantWasGranted == false) {
                    LOG.debug("GrantWasNot Granted for opeation {}. Grant = {}", op, grant);
                    if (writeCertificate != null) {
                        currentRejectedObjectCertificates.put(objectId, writeCertificate);
                    }
                    rejectedGrants.put(objectId, grant);
                    allWriteOk = false;
                } else {
                    if (writeCertificate != null) {
                        currentObjectCertificates.put(objectId, writeCertificate);
                    }
                    grants.put(objectId, grant);
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
            Utils.assertNotNull(currentObjectCertificates, "Internal error");
            if (currentObjectCertificates.size() > 0) {
                builder.putAllCurrentCertificates(currentObjectCertificates);
            }
            return builder.build();
        } else {
            final MultiGrant.Builder mgb = MultiGrant.newBuilder();
            mgb.setClientId(write1ToServer.getClientId());
            mgb.setServerId(mochiContext.getServerId());
            mgb.putAllGrants(rejectedGrants);

            final Write1RefusedFromServer.Builder builder = Write1RefusedFromServer.newBuilder();
            builder.setMultiGrant(mgb);
            Utils.assertNotNull(currentObjectCertificates, "Internal error");
            if (currentRejectedObjectCertificates.size() > 0) {
                builder.putAllCurrentCertificates(currentRejectedObjectCertificates);
            }
            return builder.build();
        } 
    }

    @Override
    public Object processWrite1ToServer(final Write1ToServer write1ToServer) {
        try {
            Object messageResponse = tryProcessWriteRegularly(write1ToServer);
            return messageResponse;
        } catch (TooOldRequestException ex) {
            LOG.info(
                    "Write1 request is too old. Denying it. Write operation number = {}, old ops operation number = {}",
                    ex.getProvidedOperationNumber(), ex.getObjectOperationNumber());
            RequestFailedFromServer.Builder rfsBuilder = RequestFailedFromServer.newBuilder();
            rfsBuilder.setType(FailureMessageType.OLD_REQUEST);
            return rfsBuilder.build();
        }
    }

    private List<String> getObjectsToWriteLock(final MultiGrant multiGrant) {
        Utils.assertNotNull(multiGrant, "MultiGrant cannot be null");
        final Set<String> objectsFromMultiGrant = multiGrant.getGrantsMap().keySet();
        Utils.assertNotNull(multiGrant, "objectsFromMultiGrant cannot be null");

        final List<String> objectsToLock = new ArrayList<String>(objectsFromMultiGrant);
        // Need to do in order to avoid deadlock
        Collections.sort(objectsToLock);
        return objectsToLock;
    }
    
    private void acquireWriteLocks(final MultiGrant multiGrant) {
        final List<String> objectsToWriteLock = getObjectsToWriteLock(multiGrant);
        for (String object : objectsToWriteLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.acquireObjectWriteLockIfNotHeld();
        }
    }
    
    private List<String> acquireWriteLocksAndReturnList(final MultiGrant multiGrant) {
        final List<String> objectsToWriteLock = getObjectsToWriteLock(multiGrant);
        for (String object : objectsToWriteLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.acquireObjectWriteLockIfNotHeld();
        }
        return objectsToWriteLock;
    }

    /*
     * Attempt to acquire locks on object which are referenced by write grants,
     * after that we check for timestamp and viewstamp to identify whether there
     * are new changes that needs to be pulled from other replicas. Also this
     * method validates whether write grants are the same. If not, it throws an
     * exception. That is a responsibiloty of the client to make sure that
     * identical grants are passed in phase 2
     */
    private Set<String> write2acquireLocksAndCheckViewStamps(final MultiGrant multiGrant) {
        final List<String> objectsToLock = acquireWriteLocksAndReturnList(multiGrant);
        LOG.debug("All locks for write grant acquired: {}", objectsToLock);

        final Set<String> listOfObjectsWhoseTimestampIsOld = new HashSet<String>();
        for (String object : objectsToLock) {
            final Grant grantForThatObject = multiGrant.getGrantsMap().get(object);
            Utils.assertNotNull(grantForThatObject, "Grant cannot be null");
            final long grantTS = grantForThatObject.getTimestamp();
            final StoreValueObjectContainer<String> storeValueContainer = getDataMap(object).get(object);
            final Long objectTS = storeValueContainer.getCurrentTimestampFromCurrentCertificate();
            LOG.debug(
                    "Got grants and certificate TS in write2acquireLocksAndCheckViewStamps. grant = [TS {}], object = [TS {}]",
                    grantTS, objectTS);

            if (objectTS == null) {
                if (storeValueContainer.isValueAvailble()) {
                    throw new IllegalStateException("objectTS is null but object value is availble");
                }
                continue;
            }
            if (objectTS < grantTS) {
                LOG.debug("Timecheck for object {} is successful", object);
                continue;
            }
            listOfObjectsWhoseTimestampIsOld.add(object);
        }
        LOG.debug("Timestamps checked. listOfObjectsWhoseTimestampIsOld = {}", listOfObjectsWhoseTimestampIsOld);

        return listOfObjectsWhoseTimestampIsOld;
    }

    private void releaseWriteLocks(final MultiGrant multiGrant) {
        final List<String> objectsToLock = getObjectsToWriteLock(multiGrant);
        for (String object : objectsToLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.releaseObjectWriteLockIfHeldByCurrent();
        }
    }
    
    private void releaseWriteLocks(List<String> objectsToUnLock) {
        for (String object : objectsToUnLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.releaseObjectWriteLockIfHeldByCurrent();
        }
    }
    
    private List<String> getObjectsToReadLock(Transaction transaction) {
        List<String> objectsFromTransaction = new ArrayList<String>();
        final List<Operation> operations = transaction.getOperationsList();
        if (operations == null)
            return objectsFromTransaction;
        
        for (Operation op : operations) {
            if (op.getAction() == OperationAction.READ) {
                final String interestedKey = op.getOperand1();
                if (StringUtils.isEmpty(interestedKey) == false) {
                    objectsFromTransaction.add(interestedKey);
                }
            }
        }

        final List<String> objectsToReadLock = new ArrayList<String>(objectsFromTransaction);
        // Need to do in order to avoid deadlock
        Collections.sort(objectsToReadLock);
        return objectsToReadLock;
    }
    
    private void acquireReadLocks(Transaction transaction) {
        final List<String> objectsToReadLock = getObjectsToReadLock(transaction);
        for (String object : objectsToReadLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.acquireObjectReadLock();
        }
    }
    
    private List<String> acquireReadLocksAndReturnList(Transaction transaction) {
        final List<String> objectsToReadLock = getObjectsToReadLock(transaction);
        for (String object : objectsToReadLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.acquireObjectReadLock();
        }
        return objectsToReadLock;
    }
    
    private void releaseReadLocks(Transaction transaction) {       
        final List<String> objectsToReleaseReadLock = getObjectsToReadLock(transaction);
        for (String object : objectsToReleaseReadLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.releaseObjectReadLock();
        }
   }
    
    private void releaseReadLocks(List<String> objectsToReleaseReadLock) {       
        for (String object : objectsToReleaseReadLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.releaseObjectReadLock();
        }
   }

    public <V, K> V getFirst(final Map<K, V> map) {
        if (map == null) {
            return null;
        }
        final Collection<V> values = map.values();
        for (final V value : values) {
            return value;
        }
        return null;
    }

    private OperationResult applyOperation(final Operation op, final WriteCertificate writeCertificateIfAny) {
        final StoreValueObjectContainer<String> storeValueContainer = getDataMap(op.getOperand1())
                .get(op.getOperand1());
        Utils.assertNotNull(storeValueContainer, "storeValueCotainer should not be null");
        if (storeValueContainer.isObjectWriteLockHeldByCurrent() == false) {
            throw new IllegalStateException("Cannot apply operation since write lock is not held");
        }
        final OperationResult.Builder resultBuilder = OperationResult.newBuilder();
        if ((op.getAction() == OperationAction.WRITE) || (op.getAction() == OperationAction.DELETE)) {
            final String oldValue;
            final boolean wasAvailable;
            final String newValue = op.getOperand2();
            Utils.assertNotNull(writeCertificateIfAny, "writeCertificate is null");
            storeValueContainer.setCurrentC(writeCertificateIfAny);
            long timestamp = storeValueContainer.getCurrentTimestampFromCurrentCertificate();
            storeValueContainer.deleteGivenWrite1Grant(timestamp);
            storeValueContainer.moveToNextEpochIfNecessary(timestamp);
            resultBuilder.setCurrentCertificate(writeCertificateIfAny);
            if (op.getAction() == OperationAction.WRITE) {
                oldValue = storeValueContainer.setValue(newValue);
                wasAvailable = storeValueContainer.setValueAvailble(true);
            } else {
                oldValue = storeValueContainer.setValue(null);
                wasAvailable = storeValueContainer.setValueAvailble(false);
            }
            resultBuilder.setExisted(wasAvailable);
            if (oldValue != null) {
                resultBuilder.setResult(oldValue);
                LOG.debug("Moving epoch to {} for key {}", storeValueContainer.getCurrentEpoch(), op.getOperand1());
            }
            final OperationResult oprationResult = resultBuilder.build();
            return oprationResult;
        } else {
            throw new UnsupportedOperationException();
        }
    }
    
    private OperationResult readOperation(final Operation op) {
        final StoreValueObjectContainer<String> storeValueContainer = getDataMap(op.getOperand1())
                .get(op.getOperand1());
        Utils.assertNotNull(storeValueContainer, "storeValueCotainer should not be null");
        if (storeValueContainer.isObjectWriteLockHeldByCurrent() == false) {
            throw new IllegalStateException("Cannot apply operation since write lock is not held");
        }
        final OperationResult.Builder resultBuilder = OperationResult.newBuilder();
        if ((op.getAction() == OperationAction.WRITE) || (op.getAction() == OperationAction.DELETE)) {
            resultBuilder.setCurrentCertificate(storeValueContainer.getCurrentC());
            resultBuilder.setExisted(true);
            resultBuilder.setResult(storeValueContainer.getValue());
            final OperationResult oprationResult = resultBuilder.build();
            return oprationResult;
        } else {
            throw new UnsupportedOperationException();
        }
    }
    
    private List<OperationResult> write2apply(final MultiGrant multiGrant, final Write2ToServer write2ToServer, final Set<String> listOfObjectsWhoseTimestampIsOld) {
        final Map<String, Grant> grantsToExecute = multiGrant.getGrantsMap();
        final List<OperationResult> operationResults = new ArrayList<OperationResult>(grantsToExecute.size());
        for (final String object : grantsToExecute.keySet()) {
            final StoreValueObjectContainer<String> storeValueContainer = getDataMap(object).get(object);
            final Grant grantForObject = grantsToExecute.get(object);
            final Transaction transaction = write2ToServer.getTransaction();
            final String txnHash = Utils.objectSHA512(transaction);
            if (grantForObject.getTransactionHash().equals(txnHash)) {
                final List<Operation> transactionOps = transaction.getOperationsList();
                for (final Operation op : transactionOps) {
                    if (op.getOperand1().equals(storeValueContainer.getKey())) {
                        if (listOfObjectsWhoseTimestampIsOld.contains(object)) {
                            operationResults.add(readOperation(op));
                        } else {
                            operationResults.add(applyOperation(op, write2ToServer.getWriteCertificate()));
                        }
                    }
                }
            } else {
                // TODO: Handle malicious client
                throw new UnsupportedOperationException();
            }
        }
        return operationResults;
    }

    @Override
    public Object processWrite2ToServer(final Write2ToServer write2ToServer) {
        final WriteCertificate wc = write2ToServer.getWriteCertificate();
        // TODO: check that all multigrants are the same
        final Map<String, MultiGrant> multiGrants = wc.getGrantsMap();
        final MultiGrant multiGrant = getFirst(multiGrants);

        final List<OperationResult> operationResults;
        try {
            final Set<String> listOfObjectsWhoseTimestampIsOld = write2acquireLocksAndCheckViewStamps(multiGrant);
            if (listOfObjectsWhoseTimestampIsOld.size() != 0) {
                LOG.debug("Found objects whose timestamps are old: {}", listOfObjectsWhoseTimestampIsOld);
            }
            LOG.debug("Timestmaps were checked, locks are held and it's time to apply the operation");
            operationResults = write2apply(multiGrant, write2ToServer, listOfObjectsWhoseTimestampIsOld);
        } catch (Exception ex) {
            LOG.error("Exception at ProcessWrite2ToServer:", ex);
            throw ex;
        } finally {
            releaseWriteLocks(multiGrant);
        }

        final Write2AnsFromServer.Builder write2AnsFromServerBuilder = Write2AnsFromServer.newBuilder();
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(operationResults);
        write2AnsFromServerBuilder.setResult(transactionResultBuilder);
        return write2AnsFromServerBuilder.build();
    }

}
