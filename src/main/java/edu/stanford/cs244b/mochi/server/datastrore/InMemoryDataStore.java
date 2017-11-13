package edu.stanford.cs244b.mochi.server.datastrore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationAction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;

public class InMemoryDataStore implements DataStore {
    private final static Logger LOG = LoggerFactory.getLogger(InMemoryDataStore.class);

    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<String, String>();

    public InMemoryDataStore() {
        // TODO: remove that later on
        data.put("DEMO_KEY_1", "value1");
        data.put("DEMO_KEY_2", "value2");
        data.put("DEMO_KEY_3", "value3");
    }

    public TransactionResult executeTransaction(Transaction transaction, boolean readOnly) {
        LOG.debug("Executing transaction: {}", transaction);
        final List<Operation> operations = transaction.getOperationsList();
        if (operations == null) {
            return null;
        }
        final List<OperationResult> operationResults = new ArrayList<OperationResult>(operations.size());
        for (Operation op : operations) {
            if (op.getAction() == OperationAction.READ) {
                final String interestedKey = op.getOperand1();
                final String valueForTheKey = data.get(interestedKey);
                final OperationResult.Builder operationResultBuilder = OperationResult.newBuilder();
                operationResultBuilder.setResult(valueForTheKey);
                operationResults.add(operationResultBuilder.build());
            } else {
                if (readOnly == true) {
                    throw new IllegalStateException("Attempt to execute non read only transactioin with readOnly flag");
                }
                // TODO
            }

        }
        final TransactionResult.Builder transactionResultBuilder = TransactionResult.newBuilder();
        transactionResultBuilder.addAllOperations(operationResults);
        return transactionResultBuilder.build();
    }

}
