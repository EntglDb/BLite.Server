// BLite.Client.Java — KV store integration tests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Integration tests for BLiteKvStore and KvBatch.
// Requires a live BLite server (gradle integrationTest).

package io.blite.client.integration;

import io.blite.client.BLiteClient;
import io.blite.client.kv.KvBatch;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.blite.client.integration.IntegrationHelpers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("KV store integration")
class KvStoreIntegrationTest {

    // Unique prefix so parallel runs never collide
    private final String PREFIX = "java_kv_" + System.currentTimeMillis() + "_";

    private BLiteClient client;
    private boolean available;

    @BeforeAll
    void setup() {
        client    = createClient();
        available = checkAvailability(client);
        if (!available) {
            System.out.println("⚠️  BLite server not reachable — kv integration tests skipped");
        }
    }

    @AfterAll
    void teardown() {
        if (available) {
            // Clean up all test keys
            var keys = client.kv.scanKeys(PREFIX);
            if (!keys.isEmpty()) {
                var batch = new KvBatch();
                for (var key : keys) batch.delete(key);
                client.kv.batch(batch);
            }
        }
        if (client != null) client.close();
    }

    private void assume() { assumeTrue(available, "⚠️  BLite server not reachable"); }

    private String k(String n) { return PREFIX + n; }
    private byte[] buf(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    // ─── set / get / exists ────────────────────────────────────────────────────

    @Test @DisplayName("set and get a value")
    void setAndGetValue() {
        assume();
        client.kv.set(k("a"), buf("hello"));
        var val = client.kv.get(k("a"));
        assertThat(val).isPresent();
        assertThat(new String(val.get(), StandardCharsets.UTF_8)).isEqualTo("hello");
    }

    @Test @DisplayName("getString convenience method")
    void getStringConvenienceMethod() {
        assume();
        client.kv.set(k("str"), "world");
        assertThat(client.kv.getString(k("str"))).contains("world");
    }

    @Test @DisplayName("get returns empty for a missing key")
    void getReturnsEmptyForMissingKey() {
        assume();
        assertThat(client.kv.get(k("no-such-key-xyz"))).isEmpty();
    }

    @Test @DisplayName("exists returns true for an existing key")
    void existsReturnsTrueForExistingKey() {
        assume();
        client.kv.set(k("exists-yes"), buf("1"));
        assertThat(client.kv.exists(k("exists-yes"))).isTrue();
    }

    @Test @DisplayName("exists returns false for a missing key")
    void existsReturnsFalseForMissingKey() {
        assume();
        assertThat(client.kv.exists(k("exists-no-xyz"))).isFalse();
    }

    // ─── delete ───────────────────────────────────────────────────────────────

    @Test @DisplayName("delete removes the key")
    void deleteRemovesKey() {
        assume();
        client.kv.set(k("del"), buf("bye"));
        client.kv.delete(k("del"));
        assertThat(client.kv.get(k("del"))).isEmpty();
    }

    // ─── scanKeys ────────────────────────────────────────────────────────────

    @Test @DisplayName("scanKeys returns keys matching the prefix")
    void scanKeysReturnsMatchingKeys() {
        assume();
        var scanPfx = k("scan_");
        client.kv.set(scanPfx + "1", buf("a"));
        client.kv.set(scanPfx + "2", buf("b"));
        client.kv.set(scanPfx + "3", buf("c"));

        var keys = client.kv.scanKeys(scanPfx);
        assertThat(keys).hasSizeGreaterThanOrEqualTo(3);
        for (var key : keys) assertThat(key).startsWith(scanPfx);
    }

    // ─── TTL / refresh ────────────────────────────────────────────────────────

    @Test @DisplayName("TTL key is present immediately after set")
    void ttlKeyPresentAfterSet() {
        assume();
        // 30-second TTL — definitely still alive inside test window
        client.kv.set(k("ttl"), buf("expires"), 30_000);
        assertThat(client.kv.exists(k("ttl"))).isTrue();
    }

    @Test @DisplayName("refresh updates the TTL of an existing key")
    void refreshUpdatesTtl() {
        assume();
        client.kv.set(k("refresh-me"), buf("data"), 30_000);
        client.kv.refresh(k("refresh-me"), 60_000); // must not throw
        assertThat(client.kv.exists(k("refresh-me"))).isTrue();
    }

    // ─── batch ────────────────────────────────────────────────────────────────

    @Test @DisplayName("batch set operations persist")
    void batchSetOperationsPersist() {
        assume();
        var count = client.kv.batch(new KvBatch()
                .set(k("bx1"), buf("one"),   0)
                .set(k("bx2"), buf("two"),   0)
                .set(k("bx3"), buf("three"), 0));
        assertThat(count).isEqualTo(3);
        assertThat(client.kv.getString(k("bx1"))).contains("one");
        assertThat(client.kv.getString(k("bx2"))).contains("two");
        assertThat(client.kv.getString(k("bx3"))).contains("three");
    }

    @Test @DisplayName("batch delete operations remove keys")
    void batchDeleteOperationsRemoveKeys() {
        assume();
        client.kv.set(k("bdel1"), buf("x"));
        client.kv.set(k("bdel2"), buf("y"));

        client.kv.batch(new KvBatch()
                .delete(k("bdel1"))
                .delete(k("bdel2")));

        assertThat(client.kv.exists(k("bdel1"))).isFalse();
        assertThat(client.kv.exists(k("bdel2"))).isFalse();
    }

    @Test @DisplayName("batch with mixed set and delete operations")
    void batchMixedOperations() {
        assume();
        client.kv.set(k("mix-keep"), buf("keep"));
        client.kv.set(k("mix-del"),  buf("remove"));

        client.kv.batch(new KvBatch()
                .set(k("mix-new"), buf("new"), 0)
                .delete(k("mix-del")));

        assertThat(client.kv.getString(k("mix-keep"))).contains("keep");
        assertThat(client.kv.getString(k("mix-new"))).contains("new");
        assertThat(client.kv.exists(k("mix-del"))).isFalse();
    }

    // ─── purgeExpired ─────────────────────────────────────────────────────────

    @Test @DisplayName("purgeExpired does not throw and returns a count")
    void purgeExpiredDoesNotThrow() {
        assume();
        var count = client.kv.purgeExpired();
        assertThat(count).isGreaterThanOrEqualTo(0);
    }
}
