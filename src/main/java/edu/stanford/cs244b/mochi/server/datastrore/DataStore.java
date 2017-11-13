package edu.stanford.cs244b.mochi.server.datastrore;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.TransactionResult;

public interface DataStore {
    public TransactionResult executeTransaction(Transaction transaction, boolean readOnly);
}
