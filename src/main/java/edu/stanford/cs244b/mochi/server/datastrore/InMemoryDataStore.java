package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.javatuples.Pair;
import org.springframework.util.StringUtils;

import edu.stanford.cs244b.mochi.server.ClusterConfiguration;
import edu.stanford.cs244b.mochi.server.MochiContext;
import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.FailureMessageType;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Grant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.MultiGrant;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationAction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResultStatus;
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

    protected boolean objectBelongsToCurrentShardServer(final String key) {
        if (key.startsWith(CONFIG_KEY_PREFIX)) {
            return true;
        }
        final List<String> serversForKey = clusterConfiguration.getServersForObject(key);
        final String currentServerId = mochiContext.getServerId();
        Utils.assertNotNull(serversForKey);
        Utils.assertNotNull(currentServerId);
        return serversForKey.contains(currentServerId);
    }

    protected OperationResult processRead(final Operation op) {
        final String interestedKey = op.getOperand1();
        checkOp1IsNonEmptyKeyError(interestedKey);
        LOG.debug("Performing processRead on key: {}", interestedKey);
        final StoreValueObjectContainer<String> keyStoreValue = getDataMap(interestedKey).get(interestedKey);
        final OperationResult.Builder operationResultBuilder = OperationResult.newBuilder();
        // TODO: do exactly the same stuff for write and delete
        if (objectBelongsToCurrentShardServer(interestedKey) == false) {
            operationResultBuilder.setStatus(OperationResultStatus.WRONG_SHARD);
            return operationResultBuilder.build();
        }
        final String valueForTheKey;
        final WriteCertificate currentC;
        if (keyStoreValue == null) {
            valueForTheKey = null;
            currentC = WriteCertificate.getDefaultInstance();
        } else {
            valueForTheKey = keyStoreValue.getValue();
            currentC = keyStoreValue.getCurrentC();
        }

        if (valueForTheKey != null) {
            operationResultBuilder.setResult(valueForTheKey);
        }
        operationResultBuilder.setExisted(keyStoreValue.isValueAvailble());
        operationResultBuilder.setCurrentCertificate(currentC);
        operationResultBuilder.setStatus(OperationResultStatus.OK);
        return operationResultBuilder.build();
    }

    protected Triplet<Grant, WriteCertificate, Boolean> processWrite(final Operation op, final String clientId,
            final Write1ToServer writeToServer) {
        final String interestedKey = op.getOperand1();
        final Grant grantAtTS;
        checkOp1IsNonEmptyKeyError(interestedKey);
        LOG.debug("Performing processWrite on key: {}", interestedKey);
        if (objectBelongsToCurrentShardServer(interestedKey) == false) {
            final Grant.Builder grantBuilder = Grant.newBuilder();
            grantBuilder.setObjectId(interestedKey);
            grantBuilder.setStatus(OperationResultStatus.WRONG_SHARD);
            grantBuilder.setTransactionHash(writeToServer.getTransactionHash());
            grantAtTS = grantBuilder.build();
            return Triplet.with(grantAtTS, null, true);
        }
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
        final List<String> objectsToWriteLock = acquireWriteLocksAndReturnList(transaction);
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
        
        releaseWriteLocks(objectsToWriteLock);

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
    
    private List<String> getObjectsToWriteLock(final Transaction transaction) {
        List<String> objectsFromTransaction = new ArrayList<String>();
        final List<Operation> operations = transaction.getOperationsList();
        if (operations == null)
            return objectsFromTransaction;
        
        for (Operation op : operations) {
            if ((op.getAction() == OperationAction.WRITE) ||
                    (op.getAction() == OperationAction.DELETE))  {
                final String interestedKey = op.getOperand1();
                if (StringUtils.isEmpty(interestedKey) == false) {
                    
                    objectsFromTransaction.add(interestedKey);
                }
            }
        }
      
        final List<String> objectsToWriteLock = new ArrayList<String>(objectsFromTransaction);
        // Need to do in order to avoid deadlock
        Collections.sort(objectsToWriteLock);
        return objectsToWriteLock;
    }
    
    private void acquireWriteLocks(final MultiGrant multiGrant) {
        final List<String> objectsToWriteLock = getObjectsToWriteLock(multiGrant);
        for (String object : objectsToWriteLock) {
            final StoreValueObjectContainer<String> storeValueContianer = getDataMap(object).get(object);
            storeValueContianer.acquireObjectWriteLockIfNotHeld();
        }
    }
    
    private List<String> acquireWriteLocksAndReturnList(Transaction transaction) {
        final List<String> objectsToReadLock = getObjectsToWriteLock(transaction);
        // Sharding proof
        for (Iterator<String> iterator = objectsToReadLock.iterator(); iterator.hasNext();) {
            String object = iterator.next();
            final StoreValueObjectContainer<String> storeValueContainer = getDataMap(object).get(object);
            if (storeValueContainer !=  null) {
                // Acquire lock if and only if SVOC is part of this server
                storeValueContainer.acquireObjectWriteLockIfNotHeld();
            } else {
                iterator.remove();
            }
        }
        return objectsToReadLock;
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
        // Sharding proof
        for (Iterator<String> iterator = objectsToReadLock.iterator(); iterator.hasNext();) {
            String object = iterator.next();
            final StoreValueObjectContainer<String> storeValueContainer = getDataMap(object).get(object);
            if (storeValueContainer !=  null) {
                // Acquire lock if and only if SVOC is part of this server
                storeValueContainer.acquireObjectReadLock();
            } else {
                iterator.remove();
            }
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
            final boolean isAvailable;
            final String newValue = op.getOperand2();
            Utils.assertNotNull(writeCertificateIfAny, "writeCertificate is null");
            storeValueContainer.setCurrentC(writeCertificateIfAny);
            long timestamp = storeValueContainer.getCurrentTimestampFromCurrentCertificate();
            storeValueContainer.deleteGivenWrite1Grant(timestamp);
            storeValueContainer.moveToNextEpochIfNecessary(timestamp);
            resultBuilder.setCurrentCertificate(writeCertificateIfAny);
            if (op.getAction() == OperationAction.WRITE) {
                storeValueContainer.setValue(newValue);
                isAvailable = storeValueContainer.setValueAvailble(true);
            } else {
                storeValueContainer.setValue(null);
                isAvailable = storeValueContainer.setValueAvailble(false);
            }
            resultBuilder.setExisted(isAvailable);
            resultBuilder.setResult(newValue);
            resultBuilder.setStatus(OperationResultStatus.OK);
            LOG.debug("Moving epoch to {} for key {}", storeValueContainer.getCurrentEpoch(), op.getOperand1());
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
            resultBuilder.setStatus(OperationResultStatus.OK);
            final OperationResult oprationResult = resultBuilder.build();
            return oprationResult;
        } else {
            throw new UnsupportedOperationException();
        }
    }
    
    private List<OperationResult> write2apply(HashMap<String, Pair<Long, List<Grant>>> coalescedTxnGrantMap, final Write2ToServer write2ToServer) {
        final Transaction transaction = write2ToServer.getTransaction();
        final List<Operation> transactionOps = transaction.getOperationsList();
        final List<OperationResult> operationResults = new ArrayList<OperationResult>(transactionOps.size());
        final String txnHash = Utils.objectSHA512(transaction);  
        for (final Operation op : transactionOps) {
            if (objectBelongsToCurrentShardServer(op.getOperand1()) == false) {
                final OperationResult.Builder operationResultBuilder = OperationResult.newBuilder();
                operationResultBuilder.setStatus(OperationResultStatus.WRONG_SHARD);
                operationResults.add(operationResultBuilder.build());
                continue;
            }
            final Grant grantForObject = coalescedTxnGrantMap.get(op.getOperand1()).getValue1().get(0);
            Utils.assertNotNull(grantForObject, "No grant for object in txn");
            Utils.assertTrue(coalescedTxnGrantMap.get(op.getOperand1()).getValue1().size() > clusterConfiguration.getServerMajority());
            if (grantForObject.getTransactionHash().equals(txnHash)) {
                final StoreValueObjectContainer<String> storeValueContainer = getDataMap(op.getOperand1()).get(op.getOperand1());
                if (op.getOperand1().equals(storeValueContainer.getKey())) {
                    Long objectTS = storeValueContainer.getCurrentTimestampFromCurrentCertificate();
                    if ( objectTS != null && objectTS > grantForObject.getTimestamp()) {
                        operationResults.add(readOperation(op));
                    } else {
                        operationResults.add(applyOperation(op, write2ToServer.getWriteCertificate()));
                    }
                } else {
                    // TODO: Handle Malicious client
                    // Previous write1 should have created the storeValueContainer
                    throw new UnsupportedOperationException();
                }
            } else {
                // TODO: Handle Malicious client
                throw new UnsupportedOperationException();
            }
        }
        return operationResults;
    }

    private HashMap<String, Pair<Long, List<Grant>>> processMultiGrantsFromAllServers(final Map<String, MultiGrant> multiGrantsFromAllServers, final Transaction transaction) {
        Utils.assertNotNull(multiGrantsFromAllServers, "MultiGrants map should not be null");
        HashMap<String, Pair<Long, List<Grant>>> coalescedTxnGrantMap = new HashMap<String, Pair<Long, List<Grant>>>();
        for (final MultiGrant multiGrant : multiGrantsFromAllServers.values()) {
            final Map<String, Grant> allGrants = multiGrant.getGrantsMap();
            Utils.assertNotNull(allGrants, "Grants map should not be null");
            final List<Operation> transactionOps = transaction.getOperationsList();
                for (final Operation op : transactionOps) {
                    final Grant grantForCurrentKey = allGrants.get(op.getOperand1());
                    if (grantForCurrentKey == null) {
                        continue;
                    }
                    long timestampFromGrant = grantForCurrentKey.getTimestamp();
                    if (coalescedTxnGrantMap.containsKey(op.getOperand1())) {
                        if (coalescedTxnGrantMap.get(op.getOperand1()).getValue0() != timestampFromGrant) {
                            throw new UnsupportedOperationException();
                        }
                        coalescedTxnGrantMap.get(op.getOperand1()).getValue1().add(grantForCurrentKey);
                    } else {
                        List<Grant> listOfGrants = new ArrayList<Grant>();
                        listOfGrants.add(grantForCurrentKey);
                        Pair<Long, List<Grant>> entry = new Pair<Long, List<Grant>>(timestampFromGrant, listOfGrants);
                        coalescedTxnGrantMap.put(op.getOperand1(), entry);
                    }
                }
        }
        return coalescedTxnGrantMap;
    }
    @Override
    public Object processWrite2ToServer(final Write2ToServer write2ToServer) {
        final WriteCertificate wc = write2ToServer.getWriteCertificate();
        final Transaction transaction = write2ToServer.getTransaction();
        // MultiGrants will be different thanks to sharding
        final Map<String, MultiGrant> multiGrantsFromAllServers = wc.getGrantsMap();
        HashMap<String, Pair<Long, List<Grant>>> coalescedTxnGrantMap = processMultiGrantsFromAllServers(wc.getGrantsMap(), transaction);       
        final List<String> objectsToWriteLock = acquireWriteLocksAndReturnList(transaction);

        final List<OperationResult> operationResults;
        try {
            LOG.debug("Timestmaps were checked, locks are held and it's time to apply the operation");
            operationResults = write2apply(coalescedTxnGrantMap, write2ToServer);
        } catch (Exception ex) {
            LOG.error("Exception at ProcessWrite2ToServer:", ex);
            throw ex;
        } finally {
            releaseWriteLocks(objectsToWriteLock);
        }

        final Write2AnsFromServer.Builder write2AnsFromServerBuilder = Write2AnsFromServer.newBuilder();
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(operationResults);
        write2AnsFromServerBuilder.setResult(transactionResultBuilder);
        return write2AnsFromServerBuilder.build();
    }

}
