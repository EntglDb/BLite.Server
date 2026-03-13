// BLite.Client.Java — UpdatePair
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client;

import io.blite.client.bson.BsonId;
import java.util.Map;

/** A document id + replacement body pair used by {@link BLiteCollection#updateBulk}. */
public record UpdatePair(BsonId id, Map<String, Object> document) {}
