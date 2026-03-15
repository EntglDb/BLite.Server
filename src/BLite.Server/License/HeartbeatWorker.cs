// BLite.Server — background service that sends hourly heartbeats to LicenseHub
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Net.Http.Json;
using System.Runtime.InteropServices;

namespace BLite.Server.License;

public sealed class HeartbeatWorker : BackgroundService
{
    private readonly ILogger<HeartbeatWorker> _log;
    private readonly LicenseManager           _license;
    private readonly InstanceIdProvider       _instance;
    private readonly IHttpClientFactory       _httpFactory;
    private readonly string                   _hubUrl;
    private readonly string                   _licenseFilePath;
    private readonly bool                     _enabled;
    private readonly TimeSpan                 _interval;
    private readonly DateTime                 _startedAt = DateTime.UtcNow;

    public HeartbeatWorker(
        IConfiguration cfg,
        LicenseManager license,
        InstanceIdProvider instance,
        IHttpClientFactory httpFactory,
        ILogger<HeartbeatWorker> log)
    {
        _log             = log;
        _license         = license;
        _instance        = instance;
        _httpFactory     = httpFactory;
        _hubUrl          = cfg.GetValue<string>("License:HubUrl")   ?? "https://licensehub.blitedb.com";
        _licenseFilePath = cfg.GetValue<string>("License:FilePath") ?? string.Empty;
        _enabled         = cfg.GetValue<bool?>("License:HeartbeatEnabled") ?? true;
        var minutes      = cfg.GetValue<int?>("License:HeartbeatIntervalMinutes") ?? 60;
        _interval        = TimeSpan.FromMinutes(Math.Max(1, minutes));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
        {
            _log.LogInformation("Heartbeat disabled via configuration.");
            return;
        }

        // Stagger start time so many instances don't all fire simultaneously
        var jitter = TimeSpan.FromSeconds(Random.Shared.Next(0, 120));
        await Task.Delay(jitter, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await SendHeartbeatAsync(stoppingToken);
            await Task.Delay(_interval, stoppingToken);
        }
    }

    private async Task SendHeartbeatAsync(CancellationToken ct)
    {
        if (_license.Current is null)
        {
            _log.LogDebug("No license loaded — skipping heartbeat.");
            return;
        }

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
                _log.LogWarning("Heartbeat returned {Status}.", (int)resp.StatusCode);
            else
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

    private sealed record HeartbeatPayload(
        string InstanceId,
        string LicenseJwt,
        string ServerVersion,
        string OperatingSystem,
        string Architecture,
        long   UptimeSeconds,
        long   TotalRequests);
}

