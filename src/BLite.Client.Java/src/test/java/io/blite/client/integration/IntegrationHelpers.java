// BLite.Client.Java — integration test helpers
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Shared utilities for integration tests.
//
// Environment variables (all optional):
//   BLITE_HOST     — server hostname   (default: localhost)
//   BLITE_PORT     — gRPC port         (default: 2626)
//   BLITE_API_KEY  — API key           (default: dev key)
//   BLITE_TLS      — 'true' for TLS    (default: false)

package io.blite.client.integration;

import io.blite.client.BLiteClient;
import io.blite.client.BLiteClientOptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

final class IntegrationHelpers {

    static final String  HOST    = System.getenv().getOrDefault("BLITE_HOST",    "localhost");
    static final int     PORT    = Integer.parseInt(System.getenv().getOrDefault("BLITE_PORT", "2626"));
    static final String  KEY     = System.getenv().getOrDefault("BLITE_API_KEY", "807ab8d026ccef15583ada824c78e538bcf1a77888566b971c244e63fe492455");
    static final boolean USE_TLS = "true".equalsIgnoreCase(System.getenv("BLITE_TLS"));

    /** Creates a fresh client using env-configured (or default) coordinates. */
    static BLiteClient createClient() {
        return new BLiteClient(new BLiteClientOptions(HOST, PORT, KEY, USE_TLS));
    }

    /**
     * Probes the server with a 3-second timeout, mirroring the TypeScript
     * {@code Promise.race([probe, timeout])} pattern.
     * Returns {@code false} (instead of throwing) on any network or auth error.
     */
    static boolean checkAvailability(BLiteClient client) {
        var future = new CompletableFuture<Boolean>();
        var thread = new Thread(() -> {
            try {
                client.listCollections();
                future.complete(true);
            } catch (Exception e) {
                future.complete(false);
            }
        });
        thread.setDaemon(true);
        thread.start();
        try {
            return future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    /** Returns a unique collection name safe to create and drop in a test suite. */
    static String uniqueCol(String tag) {
        return "java_int_" + tag + "_" + System.currentTimeMillis();
    }

    private IntegrationHelpers() {}
}
