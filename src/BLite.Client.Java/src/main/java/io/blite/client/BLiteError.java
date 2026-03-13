// BLite.Client.Java — BLiteError
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client;

/** Runtime exception wrapping a server-side or client-side BLite error. */
public class BLiteError extends RuntimeException {

    public BLiteError(String message) {
        super(message);
    }

    public BLiteError(String message, Throwable cause) {
        super(message, cause);
    }

    /** Throws {@link BLiteError} if {@code error} is non-null/non-empty. */
    public static void check(String error) {
        if (error != null && !error.isBlank()) throw new BLiteError(error);
    }
}
