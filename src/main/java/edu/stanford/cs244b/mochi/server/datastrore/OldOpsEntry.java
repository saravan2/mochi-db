package edu.stanford.cs244b.mochi.server.datastrore;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.OperationResult;

public class OldOpsEntry {
    private long operationNumber;

    // Contains result and certificate - enough to form Write2Ans response
    private OperationResult operationResult;

    public long getOperationNumber() {
        return operationNumber;
    }

    public void setOperationNumber(long operationNumber) {
        this.operationNumber = operationNumber;
    }

    public OperationResult getOperationResult() {
        return operationResult;
    }

    public void setOperationResult(OperationResult operationResult) {
        this.operationResult = operationResult;
    }

}
