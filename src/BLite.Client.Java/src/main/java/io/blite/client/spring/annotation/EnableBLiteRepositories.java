// BLite.Client.Java — @EnableBLiteRepositories
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring.annotation;

import io.blite.client.spring.repository.BLiteRepositoryFactoryBean;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enable BLite repository support in a Spring Boot application.
 *
 * <pre>{@code
 * @SpringBootApplication
 * @EnableBLiteRepositories
 * public class MyApp { ... }
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(BLiteRepositoryFactoryBean.class)
public @interface EnableBLiteRepositories {}
