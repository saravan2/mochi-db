package edu.stanford.cs244b.mochi.server.datastrore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Transaction;

public class InMemoryDataStore implements DataStore {
    private final static Logger LOG = LoggerFactory.getLogger(InMemoryDataStore.class);

    public void executeTransaction(Transaction transaction, boolean readOnly) {
        LOG.debug("Executing transaction");
    }

}
