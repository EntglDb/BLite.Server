// BLite.Client.Java — transaction integration tests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Integration tests for BLiteTransaction: commit, rollback, isolation.
// Requires a live BLite server (gradle integrationTest).

package io.blite.client.integration;

import io.blite.client.BLiteClient;
import io.blite.client.BLiteCollection;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static io.blite.client.integration.IntegrationHelpers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Transaction integration")
class TransactionIntegrationTest {

    private BLiteClient client;
    private BLiteCollection col;
    private String colName;
    private boolean available;

    @BeforeAll
    void setup() {
        client    = createClient();
        available = checkAvailability(client);
        if (!available) {
            System.out.println("⚠️  BLite server not reachable — transaction integration tests skipped");
            return;
        }
        colName = uniqueCol("txn");
        col     = client.getCollection(colName);
    }

    @AfterAll
    void teardown() {
        if (available && colName != null) {
            try { client.dropCollection(colName); } catch (Exception ignored) {}
        }
        if (client != null) client.close();
    }

    private void assume() { assumeTrue(available, "⚠️  BLite server not reachable"); }

    // ─── Commit ──────────────────────────────────────────────────────────────

    @Test @DisplayName("committed transaction persists the insert")
    void committedTransactionPersistsInsert() {
        assume();
        var tx = client.beginTransaction();
        var id = col.insert(Map.of("name", "TxnAlice", "committed", true), tx);
        tx.commit();

        var doc = col.findById(id);
        assertThat(doc).isPresent();
        assertThat(doc.get()).containsEntry("name", "TxnAlice");
    }

    @Test @DisplayName("committed transaction persists multiple inserts")
    void committedTransactionPersistsMultipleInserts() {
        assume();
        var tx  = client.beginTransaction();
        var ids = col.insertBulk(List.of(
                Map.<String, Object>of("name", "Multi1", "tx", true),
                Map.<String, Object>of("name", "Multi2", "tx", true)
        ), tx);
        tx.commit();

        for (var id : ids) assertThat(col.findById(id)).isPresent();
    }

    @Test @DisplayName("commit sets transaction state correctly")
    void commitSetsStateCorrectly() {
        assume();
        var tx = client.beginTransaction();
        col.insert(Map.of("ping", true), tx);
        tx.commit();

        assertThat(tx.isCommitted()).isTrue();
        assertThat(tx.isActive()).isFalse();
        assertThat(tx.isRolledBack()).isFalse();
    }

    // ─── Rollback ────────────────────────────────────────────────────────────

    @Test @DisplayName("rolled-back transaction does not persist the insert")
    void rolledBackTransactionDoesNotPersist() {
        assume();
        var tx = client.beginTransaction();
        var id = col.insert(Map.of("name", "TxnGhost", "committed", false), tx);
        tx.rollback();

        assertThat(col.findById(id)).isEmpty();
    }

    @Test @DisplayName("rollback sets transaction state correctly")
    void rollbackSetsStateCorrectly() {
        assume();
        var tx = client.beginTransaction();
        col.insert(Map.of("temp", true), tx);
        tx.rollback();

        assertThat(tx.isRolledBack()).isTrue();
        assertThat(tx.isActive()).isFalse();
        assertThat(tx.isCommitted()).isFalse();
    }

    // ─── Auto-rollback on close ────────────────────────────────────────────

    @Test @DisplayName("try-with-resources rolls back uncommitted transaction")
    void tryWithResourcesRollsBackUncommitted() {
        assume();
        var idRef = new java.util.concurrent.atomic.AtomicReference<io.blite.client.bson.BsonId>();
        try (var tx = client.beginTransaction()) {
            idRef.set(col.insert(Map.of("name", "AutoRollback"), tx));
            // intentionally not committing
        }

        assertThat(col.findById(idRef.get())).isEmpty();
    }

    // ─── Double-commit guard ──────────────────────────────────────────────────

    @Test @DisplayName("committing twice throws IllegalStateException")
    void committingTwiceThrows() {
        assume();
        var tx = client.beginTransaction();
        col.insert(Map.of("val", 1), tx);
        tx.commit();

        assertThatThrownBy(tx::commit).isInstanceOf(IllegalStateException.class);
    }

    @Test @DisplayName("rollback after commit is a no-op")
    void rollbackAfterCommitIsNoOp() {
        assume();
        var tx = client.beginTransaction();
        col.insert(Map.of("val", 2), tx);
        tx.commit();

        // rollback() is idempotent — must not throw
        tx.rollback();
        assertThat(tx.isRolledBack()).isFalse();
        assertThat(tx.isCommitted()).isTrue();
    }
}
