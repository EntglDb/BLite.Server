// BLite.Client.Java — @BLiteId
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring.annotation;

import java.lang.annotation.*;

/**
 * Marks the field that holds the document's {@link io.blite.client.bson.BsonId}.
 * The field type must be {@code BsonId} (or {@code String} for string IDs).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BLiteId {}
