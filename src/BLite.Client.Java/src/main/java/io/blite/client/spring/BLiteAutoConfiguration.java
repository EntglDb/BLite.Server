// BLite.Client.Java — BLiteAutoConfiguration
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring;

import io.blite.client.BLiteClient;
import io.blite.client.BLiteClientOptions;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(BLiteProperties.class)
public class BLiteAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public BLiteClient bliteClient(BLiteProperties props) {
        return new BLiteClient(new BLiteClientOptions(
                props.getHost(),
                props.getPort(),
                props.getApiKey(),
                props.isUseTls()));
    }

    @Bean
    @ConditionalOnMissingBean
    public BLiteTemplate bliteTemplate(BLiteClient client) {
        return new BLiteTemplate(client);
    }
}
