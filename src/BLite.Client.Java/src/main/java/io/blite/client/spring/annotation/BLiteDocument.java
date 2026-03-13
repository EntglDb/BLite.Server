// BLite.Client.Java — @BLiteDocument
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring.annotation;

import java.lang.annotation.*;

/**
 * Marks a class as a BLite document and specifies its collection name.
 *
 * <pre>{@code
 * @BLiteDocument("products")
 * public class Product {
 *     @BLiteId public String id;
 *     public String name;
 *     public double price;
 * }
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BLiteDocument {
    /** The collection name in BLite this type is stored in. */
    String value();
}
