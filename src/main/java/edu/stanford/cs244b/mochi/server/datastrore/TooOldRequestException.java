package edu.stanford.cs244b.mochi.server.datastrore;

public class TooOldRequestException extends RuntimeException {
    private static final long serialVersionUID = -8502216672025808772L;

    final long providedOperationNumber;
    final long objectOperationNumber;

    public TooOldRequestException(final long providedOpNum, final long objectOpNum) {
        providedOperationNumber = providedOpNum;
        objectOperationNumber = objectOpNum;
    }

    public long getProvidedOperationNumber() {
        return providedOperationNumber;
    }

    public long getObjectOperationNumber() {
        return objectOperationNumber;
    }
}
