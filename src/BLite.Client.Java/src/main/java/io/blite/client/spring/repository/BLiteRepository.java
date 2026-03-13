// BLite.Client.Java — BLiteRepository
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring.repository;

import io.blite.client.bson.BsonId;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Base repository interface for BLite entities.
 *
 * <pre>{@code
 * public interface ProductRepository extends BLiteRepository<Product, BsonId> {}
 * }</pre>
 */
@NoRepositoryBean
public interface BLiteRepository<T, ID> extends CrudRepository<T, ID> {}
