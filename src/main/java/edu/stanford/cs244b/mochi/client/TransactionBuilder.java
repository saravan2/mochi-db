package edu.stanford.cs244b.mochi.client;

import java.util.ArrayList;
import java.util.List;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationAction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;

public class TransactionBuilder {
    private final List<Operation> operations = new ArrayList<Operation>();
    
    private TransactionBuilder() {
    }

    public static TransactionBuilder startNewTransaction() {
        return new TransactionBuilder();
    }

    public TransactionBuilder addWriteOperation(final String key, final String value, int operationNumber) {
        final Operation.Builder oBuilder = Operation.newBuilder();
        oBuilder.setAction(OperationAction.WRITE);
        oBuilder.setOperand1(key);
        oBuilder.setOperand2(value);
        oBuilder.setOperationNumber(operationNumber);
        operations.add(oBuilder.build());
        return this;
    }

    public Transaction build() {
        final Transaction.Builder tBuilder = Transaction.newBuilder();
        for (final Operation op : operations) {
            tBuilder.addOperations(op);
        }
        return tBuilder.build();
    }
}
