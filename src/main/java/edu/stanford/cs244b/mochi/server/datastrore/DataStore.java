package edu.stanford.cs244b.mochi.server.datastrore;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;

public interface DataStore {
    public void executeTransaction(Transaction transaction, boolean readOnly);
}
