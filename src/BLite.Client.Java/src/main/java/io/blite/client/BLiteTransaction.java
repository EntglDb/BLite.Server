// BLite.Client.Java — BLiteTransaction
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client;

import io.blite.proto.v1.TransactionServiceGrpc;
import io.blite.proto.v1.TransactionRequest;

/**
 * Represents an active server-side transaction.
 * Use with try-with-resources — uncommitted transactions are automatically rolled back on close.
 */
public final class BLiteTransaction implements AutoCloseable {

    private final String transactionId;
    private final TransactionServiceGrpc.TransactionServiceBlockingStub stub;

    private boolean committed  = false;
    private boolean rolledBack = false;

    BLiteTransaction(String transactionId, TransactionServiceGrpc.TransactionServiceBlockingStub stub) {
        this.transactionId = transactionId;
        this.stub          = stub;
    }

    public String getTransactionId() { return transactionId; }
    public boolean isActive()        { return !committed && !rolledBack; }
    public boolean isCommitted()     { return committed; }
    public boolean isRolledBack()    { return rolledBack; }

    public void commit() {
        if (!isActive()) throw new IllegalStateException("Transaction is no longer active");
        stub.commit(TransactionRequest.newBuilder().setTransactionId(transactionId).build());
        committed = true;
    }

    public void rollback() {
        if (!isActive()) return; // idempotent rollback
        stub.rollback(TransactionRequest.newBuilder().setTransactionId(transactionId).build());
        rolledBack = true;
    }

    @Override
    public void close() {
        if (isActive()) rollback();
    }
}
