// BLite.Client.Java — BLiteRepositoryFactoryBean
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring.repository;

import io.blite.client.spring.BLiteTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.ParameterizedType;

/**
 * Spring {@code @Configuration} that scans for {@link BLiteRepository} sub-interfaces
 * and registers a {@link BLiteRepositoryImpl} bean for each one found in the application
 * context (registered via {@link io.blite.client.spring.annotation.EnableBLiteRepositories}).
 *
 * <p>This is a simplified factory approach: it relies on Spring's bean-definition
 * introspection rather than a full {@code RepositoryFactorySupport} hierarchy,
 * keeping the v1 scope minimal and dependency-free (no Spring Data proxy magic).
 * Spring Data's {@code CrudRepository} contract is satisfied by delegation.
 */
@Configuration
public class BLiteRepositoryFactoryBean {

    @Autowired
    private ApplicationContext ctx;

    @Autowired
    private BLiteTemplate template;

    /**
     * Returns a {@link BLiteRepositoryImpl} instance for the given repository interface.
     * Call site: application-level {@code @Bean} methods that declare the repository type.
     *
     * <pre>{@code
     * @Bean
     * public ProductRepository productRepository(BLiteRepositoryFactoryBean factory) {
     *     return factory.getRepository(ProductRepository.class);
     * }
     * }</pre>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T, R extends BLiteRepository<T, ?>> R getRepository(Class<R> repositoryInterface) {
        // Extract T from BLiteRepository<T, ID>
        for (var iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType pt
                    && BLiteRepository.class.isAssignableFrom((Class<?>) pt.getRawType())) {
                var entityType = (Class<T>) pt.getActualTypeArguments()[0];
                return (R) new BLiteRepositoryImpl<>(template, entityType);
            }
        }
        throw new IllegalArgumentException(
                repositoryInterface.getSimpleName() + " does not extend BLiteRepository<T,ID>");
    }
}
