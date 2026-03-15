// BLite.Server — background service that sends hourly heartbeats to LicenseHub
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BLite.Server.License;

public sealed class HeartbeatWorker : BackgroundService
{
    // Heartbeat is always enabled and cannot be disabled by end-users.
    // This allows the EntglDb team to monitor active deployments and detect
    // potential license/ToS violations.
    // Policy: if a valid license is present, missed heartbeats do NOT cause
    // any restriction on server functionality — the heartbeat is telemetry only.
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(60);

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    private readonly ILogger<HeartbeatWorker> _log;
    private readonly LicenseManager           _license;
    private readonly InstanceIdProvider       _instance;
    private readonly IHttpClientFactory       _httpFactory;
    private readonly RestrictionService       _restrictions;
    private readonly string                   _hubUrl;
    private readonly string                   _licenseFilePath;
    private readonly DateTime                 _startedAt = DateTime.UtcNow;

    public HeartbeatWorker(
        IConfiguration cfg,
        LicenseManager license,
        InstanceIdProvider instance,
        IHttpClientFactory httpFactory,
        RestrictionService restrictions,
        ILogger<HeartbeatWorker> log)
    {
        _log             = log;
        _license         = license;
        _instance        = instance;
        _httpFactory     = httpFactory;
        _restrictions    = restrictions;
        _hubUrl          = cfg.GetValue<string>("License:HubUrl")   ?? "https://licensehub.blitedb.com";
        _licenseFilePath = cfg.GetValue<string>("License:FilePath") ?? string.Empty;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Stagger start time so many instances don't all fire simultaneously
        var jitter = TimeSpan.FromSeconds(Random.Shared.Next(0, 120));
        await Task.Delay(jitter, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await SendHeartbeatAsync(stoppingToken);
            await Task.Delay(Interval, stoppingToken);
        }
    }

    private async Task SendHeartbeatAsync(CancellationToken ct)
    {

        try
        {
            var jwt = !string.IsNullOrEmpty(_licenseFilePath) && File.Exists(_licenseFilePath)
                ? (await File.ReadAllTextAsync(_licenseFilePath, ct)).Trim()
                : string.Empty;

            var payload = new HeartbeatPayload(
                InstanceId:     _instance.InstanceId,
                LicenseJwt:     jwt,
                ServerVersion:  GetVersion(),
                OperatingSystem: RuntimeInformation.OSDescription,
                Architecture:    RuntimeInformation.OSArchitecture.ToString(),
                UptimeSeconds:  (long)(DateTime.UtcNow - _startedAt).TotalSeconds,
                TotalRequests:  0);

            using var client = _httpFactory.CreateClient("heartbeat");
            var resp = await client.PostAsJsonAsync(
                new Uri(new Uri(_hubUrl.TrimEnd('/') + "/"), "api/v1/heartbeat"),
                payload, ct);

            if (!resp.IsSuccessStatusCode)
            {
                _log.LogWarning("Heartbeat returned {Status}.", (int)resp.StatusCode);
                return;
            }

            var body = await resp.Content.ReadFromJsonAsync<HeartbeatResponseDto>(JsonOpts, ct);
            if (body?.Restrictions is { } r)
            {
                var snapshot = new RestrictionSnapshot
                {
                    OperationDelayMs  = Math.Max(0, r.OperationDelayMs),
                    QueryResultLimit  = Math.Max(0, r.QueryResultLimit),
                    DisableQueryCache = r.DisableQueryCache,
                    WarnBannerMessage = r.WarnBannerMessage,
                };
                _restrictions.Update(snapshot);
                if (snapshot.HasAny)
                    _log.LogWarning("Restrictions active: delay={Delay}ms, resultLimit={Limit}, cacheOff={CacheOff}, banner={Banner}",
                        snapshot.OperationDelayMs, snapshot.QueryResultLimit,
                        snapshot.DisableQueryCache, snapshot.WarnBannerMessage is not null);
            }
            else
            {
                // Hub returned no restrictions — clear any previously active ones
                _restrictions.Update(RestrictionSnapshot.None);
            }

            _log.LogDebug("Heartbeat sent for instance {Id}.", _instance.InstanceId[..8]);
        }
        catch (OperationCanceledException) { /* shutting down */ }
        catch (Exception ex)
        {
            _log.LogWarning("Heartbeat failed: {Msg}", ex.Message);
        }
    }

    private static string GetVersion()
        => typeof(HeartbeatWorker).Assembly.GetName().Version?.ToString() ?? "0.0.0";

    // Minimal DTO to deserialise only what we need from the heartbeat response
    private sealed class HeartbeatResponseDto
    {
        public string? LicenseStatus { get; set; }
        public string? Message { get; set; }
        public RestrictionsDto? Restrictions { get; set; }
    }

    private sealed class RestrictionsDto
    {
        public int    OperationDelayMs  { get; set; } = 0;
        public int    QueryResultLimit  { get; set; } = 0;
        public bool   DisableQueryCache { get; set; } = false;
        public string? WarnBannerMessage { get; set; } = null;
    }

    private sealed record HeartbeatPayload(
        string InstanceId,
        string LicenseJwt,
        string ServerVersion,
        string OperatingSystem,
        string Architecture,
        long   UptimeSeconds,
        long   TotalRequests);
}

