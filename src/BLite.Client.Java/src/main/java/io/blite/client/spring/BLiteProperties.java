// BLite.Client.Java — BLiteProperties
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for BLite.
 *
 * <pre>{@code
 * spring:
 *   blite:
 *     host: localhost
 *     port: 2626
 *     api-key: your-api-key
 *     use-tls: false
 * }</pre>
 */
@ConfigurationProperties(prefix = "spring.blite")
public class BLiteProperties {

    private String  host   = "localhost";
    private int     port   = 2626;
    private String  apiKey;
    private boolean useTls = true;

    public String  getHost()   { return host; }
    public int     getPort()   { return port; }
    public String  getApiKey() { return apiKey; }
    public boolean isUseTls()  { return useTls; }

    public void setHost(String host)     { this.host   = host;   }
    public void setPort(int port)        { this.port   = port;   }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }
    public void setUseTls(boolean tls)   { this.useTls = tls;   }
}
