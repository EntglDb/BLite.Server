// BLite.Client.Java — BLiteClientOptions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client;

/** Connection and authentication options for {@link BLiteClient}. */
public final class BLiteClientOptions {

    private String  host   = "localhost";
    private int     port   = 2626;
    private String  apiKey;
    private boolean useTls = true;

    public BLiteClientOptions() {}

    public BLiteClientOptions(String host, int port, String apiKey, boolean useTls) {
        this.host   = host;
        this.port   = port;
        this.apiKey = apiKey;
        this.useTls = useTls;
    }

    public String  getHost()   { return host; }
    public int     getPort()   { return port; }
    public String  getApiKey() { return apiKey; }
    public boolean isUseTls()  { return useTls; }

    public BLiteClientOptions host(String host)     { this.host = host;     return this; }
    public BLiteClientOptions port(int port)        { this.port = port;     return this; }
    public BLiteClientOptions apiKey(String apiKey) { this.apiKey = apiKey; return this; }
    public BLiteClientOptions useTls(boolean tls)   { this.useTls = tls;   return this; }

    /** Convenience: insecure localhost connection (for dev/testing). */
    public static BLiteClientOptions local(String apiKey) {
        return new BLiteClientOptions("localhost", 2626, apiKey, false);
    }
}
