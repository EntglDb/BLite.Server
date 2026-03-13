// BLite.Client.Java — CRUD integration tests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Integration tests for BLiteCollection CRUD and bulk operations.
// Requires a live BLite server (gradle integrationTest).

package io.blite.client.integration;

import io.blite.client.BLiteClient;
import io.blite.client.BLiteCollection;
import io.blite.client.UpdatePair;
import io.blite.client.bson.BsonId;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static io.blite.client.integration.IntegrationHelpers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("CRUD integration")
class CrudIntegrationTest {

    private BLiteClient client;
    private BLiteCollection col;
    private String colName;
    private boolean available;

    @BeforeAll
    void setup() {
        client    = createClient();
        available = checkAvailability(client);
        if (!available) {
            System.out.println("⚠️  BLite server not reachable — crud integration tests skipped");
            return;
        }
        colName = uniqueCol("crud");
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

    // ─── insert / findById ────────────────────────────────────────────────────

    @Test @DisplayName("insert returns a valid BsonId")
    void insertReturnsValidBsonId() {
        assume();
        var id = col.insert(Map.of("name", "Alice", "age", 30));
        assertThat(id).isNotNull();
        assertThat(id.toString()).isNotBlank();
    }

    @Test @DisplayName("findById returns the inserted document")
    void findByIdReturnsInsertedDocument() {
        assume();
        var id  = col.insert(Map.of("name", "Bob", "age", 25));
        var doc = col.findById(id);
        assertThat(doc).isPresent();
        assertThat(doc.get()).containsEntry("name", "Bob").containsEntry("age", 25);
    }

    @Test @DisplayName("findById returns empty for an unknown id")
    void findByIdReturnsEmptyForUnknownId() {
        assume();
        var ghostId = col.insert(Map.of("_placeholder", 1));
        col.delete(ghostId);
        var doc = col.findById(ghostId);
        assertThat(doc).isEmpty();
    }

    // ─── update ───────────────────────────────────────────────────────────────

    @Test @DisplayName("update changes fields and findById reflects the change")
    void updateChangesFields() {
        assume();
        var id = col.insert(Map.of("name", "Carol", "score", 10));
        var ok = col.update(id, Map.of("name", "Carol", "score", 99));
        assertThat(ok).isTrue();
        var doc = col.findById(id);
        assertThat(doc).isPresent();
        assertThat(doc.get()).containsEntry("score", 99);
    }

    @Test @DisplayName("update returns false for an unknown id")
    void updateReturnsFalseForUnknownId() {
        assume();
        var ghostId = col.insert(Map.of("_placeholder", 2));
        col.delete(ghostId);
        var ok = col.update(ghostId, Map.of("name", "Ghost"));
        assertThat(ok).isFalse();
    }

    // ─── delete ───────────────────────────────────────────────────────────────

    @Test @DisplayName("delete removes the document")
    void deleteRemovesDocument() {
        assume();
        var id = col.insert(Map.of("name", "Dave", "temp", true));
        var ok = col.delete(id);
        assertThat(ok).isTrue();
        assertThat(col.findById(id)).isEmpty();
    }

    @Test @DisplayName("delete returns false for an unknown id")
    void deleteReturnsFalseForUnknownId() {
        assume();
        var ghostId = col.insert(Map.of("_placeholder", 3));
        col.delete(ghostId);
        var ok = col.delete(ghostId);
        assertThat(ok).isFalse();
    }

    // ─── insertBulk ───────────────────────────────────────────────────────────

    @Test @DisplayName("insertBulk returns one id per document")
    void insertBulkReturnsOneIdPerDocument() {
        assume();
        var docs = List.of(
                Map.<String, Object>of("name", "Eva",   "city", "Rome"),
                Map.<String, Object>of("name", "Frank", "city", "Milan"),
                Map.<String, Object>of("name", "Grace", "city", "Turin")
        );
        var ids = col.insertBulk(docs);
        assertThat(ids).hasSize(3);
        for (var id : ids) assertThat(id).isNotNull();
    }

    @Test @DisplayName("insertBulk with empty list returns empty")
    void insertBulkWithEmptyListReturnsEmpty() {
        assume();
        assertThat(col.insertBulk(List.of())).isEmpty();
    }

    // ─── deleteBulk ───────────────────────────────────────────────────────────

    @Test @DisplayName("deleteBulk removes multiple documents at once")
    void deleteBulkRemovesMultipleDocuments() {
        assume();
        var ids = col.insertBulk(List.of(
                Map.<String, Object>of("name", "X1", "batch", true),
                Map.<String, Object>of("name", "X2", "batch", true),
                Map.<String, Object>of("name", "X3", "batch", true)
        ));
        var count = col.deleteBulk(ids);
        assertThat(count).isEqualTo(3);
        for (var id : ids) assertThat(col.findById(id)).isEmpty();
    }

    @Test @DisplayName("deleteBulk with empty list returns 0")
    void deleteBulkWithEmptyListReturnsZero() {
        assume();
        assertThat(col.deleteBulk(List.<BsonId>of())).isEqualTo(0);
    }

    // ─── updateBulk ───────────────────────────────────────────────────────────

    @Test @DisplayName("updateBulk applies all changes")
    void updateBulkAppliesAllChanges() {
        assume();
        var ids = col.insertBulk(List.of(
                Map.<String, Object>of("name", "P1", "v", 1),
                Map.<String, Object>of("name", "P2", "v", 2)
        ));
        var pairs = List.of(
                new UpdatePair(ids.get(0), Map.of("name", "P1", "v", 10)),
                new UpdatePair(ids.get(1), Map.of("name", "P2", "v", 20))
        );
        var count = col.updateBulk(pairs);
        assertThat(count).isEqualTo(2);
        assertThat(col.findById(ids.get(0)).get()).containsEntry("v", 10);
        assertThat(col.findById(ids.get(1)).get()).containsEntry("v", 20);
    }
}
