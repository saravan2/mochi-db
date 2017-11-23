package edu.stanford.cs244b.mochi.client;

import java.util.ArrayList;
import java.util.List;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Operation;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationAction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;

public class TransactionBuilder {
    private final List<Operation> operations = new ArrayList<Operation>();
    private final long operationNumber;
    
    private TransactionBuilder(long operationNumber) {
        this.operationNumber = operationNumber;
    }

    public static TransactionBuilder startNewTransaction(long operationNumber) {
        return new TransactionBuilder(operationNumber);
    }

    public TransactionBuilder addWriteOperation(final String key, final String value) {
        final Operation.Builder oBuilder = Operation.newBuilder();
        oBuilder.setAction(OperationAction.WRITE);
        oBuilder.setOperand1(key);
        oBuilder.setOperand2(value);
        oBuilder.setOperationNumber(operationNumber);
        operations.add(oBuilder.build());
        return this;
    }
    
    public TransactionBuilder addReadOperation(final String key) {
        final Operation.Builder oBuilder = Operation.newBuilder();
        oBuilder.setAction(OperationAction.READ);
        oBuilder.setOperand1(key);
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
