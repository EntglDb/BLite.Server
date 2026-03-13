// BLite.Client.Java — query integration tests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Integration tests for QueryBuilder: filters, sorting, and pagination.
// Requires a live BLite server (gradle integrationTest).

package io.blite.client.integration;

import io.blite.client.BLiteClient;
import io.blite.client.BLiteCollection;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static io.blite.client.integration.IntegrationHelpers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Query integration")
class QueryIntegrationTest {

    // Seed data — 6 users with distinct names, ages, statuses, and cities
    private static final List<Map<String, Object>> SEED = List.of(
            Map.of("name", "Alice", "age", 30, "status", "active",   "city", "Rome"),
            Map.of("name", "Bob",   "age", 25, "status", "inactive", "city", "Milan"),
            Map.of("name", "Carol", "age", 35, "status", "active",   "city", "Turin"),
            Map.of("name", "Dave",  "age", 22, "status", "inactive", "city", "Naples"),
            Map.of("name", "Eve",   "age", 28, "status", "active",   "city", "Rome"),
            Map.of("name", "Frank", "age", 40, "status", "banned",   "city", "Milan")
    );

    private BLiteClient client;
    private BLiteCollection col;
    private String colName;
    private boolean available;

    @BeforeAll
    void setup() {
        client    = createClient();
        available = checkAvailability(client);
        if (!available) {
            System.out.println("⚠️  BLite server not reachable — query integration tests skipped");
            return;
        }
        colName = uniqueCol("query");
        col     = client.getCollection(colName);
        col.insertBulk(SEED);
    }

    @AfterAll
    void teardown() {
        if (available && colName != null) {
            try { client.dropCollection(colName); } catch (Exception ignored) {}
        }
        if (client != null) client.close();
    }

    private void assume() { assumeTrue(available, "⚠️  BLite server not reachable"); }

    private List<Map<String, Object>> q() {
        return col.query().toList();
    }

    // ─── whereEq ─────────────────────────────────────────────────────────────

    @Test @DisplayName("whereEq filters by exact string match")
    void whereEqFiltersExactMatch() {
        assume();
        var docs = col.query().whereEq("status", "active").toList();
        assertThat(docs).hasSize(3);
        for (var d : docs) assertThat(d.get("status")).isEqualTo("active");
    }

    @Test @DisplayName("whereEq with no match returns empty list")
    void whereEqNoMatchReturnsEmpty() {
        assume();
        assertThat(col.query().whereEq("status", "suspended").toList()).isEmpty();
    }

    // ─── whereNeq ────────────────────────────────────────────────────────────

    @Test @DisplayName("whereNeq excludes matching documents")
    void whereNeqExcludesMatching() {
        assume();
        var docs = col.query().whereNeq("status", "inactive").toList();
        for (var d : docs) assertThat(d.get("status")).isNotEqualTo("inactive");
        assertThat(docs).hasSize(4); // active×3 + banned×1
    }

    // ─── whereGt / whereLt ───────────────────────────────────────────────────

    @Test @DisplayName("whereGt filters documents with field > value")
    void whereGtFiltersAbove() {
        assume();
        var docs = col.query().whereGt("age", 30).toList();
        assertThat(docs).hasSize(2); // Carol 35, Frank 40
        for (var d : docs) assertThat((int) d.get("age")).isGreaterThan(30);
    }

    @Test @DisplayName("whereLte filters documents with field <= value")
    void whereLteFiltersAtMost() {
        assume();
        var docs = col.query().whereLte("age", 25).toList();
        assertThat(docs).hasSize(2); // Bob 25, Dave 22
        for (var d : docs) assertThat((int) d.get("age")).isLessThanOrEqualTo(25);
    }

    @Test @DisplayName("whereGte + whereLte: age range [25, 35]")
    void whereGteLteAgeRange() {
        assume();
        var docs = col.query().whereGte("age", 25).whereLte("age", 35).toList();
        assertThat(docs).hasSize(4); // Alice 30, Bob 25, Carol 35, Eve 28
        for (var d : docs) {
            assertThat((int) d.get("age")).isGreaterThanOrEqualTo(25);
            assertThat((int) d.get("age")).isLessThanOrEqualTo(35);
        }
    }

    // ─── whereStartsWith / whereContains ─────────────────────────────────────

    @Test @DisplayName("whereStartsWith matches names beginning with A")
    void whereStartsWithMatchesPrefix() {
        assume();
        var docs = col.query().whereStartsWith("name", "A").toList();
        assertThat(docs).hasSize(1);
        assertThat(docs.get(0).get("name")).isEqualTo("Alice");
    }

    @Test @DisplayName("whereContains matches substring in city")
    void whereContainsMatchesSubstring() {
        assume();
        var docs = col.query().whereContains("city", "Rome").toList();
        // Alice + Eve — 'Rome' contains 'Rome'
        assertThat(docs).hasSize(2);
        for (var d : docs) assertThat((String) d.get("city")).contains("Rome");
    }

    // ─── whereIn ─────────────────────────────────────────────────────────────

    @Test @DisplayName("whereIn matches documents with field in list")
    void whereInMatchesList() {
        assume();
        var docs = col.query().whereIn("status", List.of("active", "banned")).toList();
        assertThat(docs).hasSize(4); // Alice, Carol, Eve, Frank
        for (var d : docs)
            assertThat((String) d.get("status")).isIn("active", "banned");
    }

    // ─── orderBy ─────────────────────────────────────────────────────────────

    @Test @DisplayName("orderBy ascending sorts by field")
    void orderByAscendingSortsByField() {
        assume();
        var docs = col.query().orderBy("age").toList();
        var ages = docs.stream().map(d -> (int) d.get("age")).toList();
        for (int i = 1; i < ages.size(); i++)
            assertThat(ages.get(i)).isGreaterThanOrEqualTo(ages.get(i - 1));
    }

    @Test @DisplayName("orderByDescending sorts by field descending")
    void orderByDescendingSortsByFieldDesc() {
        assume();
        var docs = col.query().orderByDescending("age").toList();
        var ages = docs.stream().map(d -> (int) d.get("age")).toList();
        for (int i = 1; i < ages.size(); i++)
            assertThat(ages.get(i)).isLessThanOrEqualTo(ages.get(i - 1));
    }

    // ─── skip / take ─────────────────────────────────────────────────────────

    @Test @DisplayName("take limits the number of results")
    void takeLimitsResults() {
        assume();
        var docs = col.query().orderBy("age").take(2).toList();
        assertThat(docs).hasSize(2);
    }

    @Test @DisplayName("skip skips the first N results")
    void skipSkipsFirstN() {
        assume();
        var all     = col.query().orderBy("age").toList();
        var skipped = col.query().orderBy("age").skip(2).toList();
        assertThat(skipped).hasSize(all.size() - 2);
        assertThat(skipped.get(0).get("age")).isEqualTo(all.get(2).get("age"));
    }

    @Test @DisplayName("skip + take paginates correctly")
    void skipAndTakePaginate() {
        assume();
        var page = col.query().orderBy("age").skip(1).take(2).toList();
        assertThat(page).hasSize(2);
    }

    // ─── findAll ─────────────────────────────────────────────────────────────

    @Test @DisplayName("findAll returns all seeded documents")
    void findAllReturnsAllDocuments() {
        assume();
        var docs = col.query().toList();
        assertThat(docs).hasSizeGreaterThanOrEqualTo(SEED.size());
    }
}
