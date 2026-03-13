// BLite.Client.Java — CreateUserResult
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.admin;

/** Result of a user creation or key-rotation call. The plaintext key is shown once. */
public record CreateUserResult(String apiKey, String error) {

    public boolean isSuccess() { return error == null || error.isBlank(); }
}
