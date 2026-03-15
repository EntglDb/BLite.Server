// BLite.Server — loads and validates the RS256 license JWT
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;

namespace BLite.Server.License;

public enum LicenseLoadResult { Ok, NotConfigured, FileNotFound, Invalid, Expired }

public sealed class LicenseManager
{
    private readonly ILogger<LicenseManager> _log;
    private readonly string                  _filePath;
    private readonly string                  _publicKeyPem;

    public LicenseInfo?      Current { get; private set; }
    public LicenseLoadResult Result  { get; private set; } = LicenseLoadResult.NotConfigured;

    public LicenseManager(IConfiguration cfg, ILogger<LicenseManager> log)
    {
        _log          = log;
        _filePath     = cfg.GetValue<string>("License:FilePath")    ?? string.Empty;
        _publicKeyPem = cfg.GetValue<string>("License:PublicKeyPem") ?? string.Empty;
        Reload();
    }

    public void Reload()
    {
        Current = null;
        Result  = LicenseLoadResult.NotConfigured;

        if (string.IsNullOrWhiteSpace(_filePath))
        {
            _log.LogInformation("No license file configured — running under AGPL-3.0.");
            return;
        }

        if (!File.Exists(_filePath))
        {
            _log.LogWarning("License file not found at {Path}.", _filePath);
            Result = LicenseLoadResult.FileNotFound;
            return;
        }

        var jwt = File.ReadAllText(_filePath).Trim();
        if (!TryValidate(jwt, out var info, out var reason))
        {
            _log.LogWarning("License validation failed: {Reason}", reason);
            Result = LicenseLoadResult.Invalid;
            return;
        }

        if (info!.ExpiresAt < DateTime.UtcNow)
        {
            _log.LogWarning("License expired on {Date:yyyy-MM-dd}.", info.ExpiresAt);
            Result  = LicenseLoadResult.Expired;
            Current = info;
            return;
        }

        Current = info;
        Result  = LicenseLoadResult.Ok;
        _log.LogInformation(
            "License loaded: tier={Tier}, expires={Date:yyyy-MM-dd}, instances={N}",
            info.Tier, info.ExpiresAt, info.MaxInstances);
    }

    // ── JWT validation ────────────────────────────────────────────────────────

    private bool TryValidate(string jwt, out LicenseInfo? info, out string reason)
    {
        info   = null;
        reason = string.Empty;

        RSA? rsa = null;

        try
        {
            if (string.IsNullOrWhiteSpace(_publicKeyPem))
            {
                reason = "License:PublicKeyPem is not configured.";
                return false;
            }

            rsa = RSA.Create();
            rsa.ImportFromPem(_publicKeyPem);

            var handler    = new JwtSecurityTokenHandler();
            var parameters = new TokenValidationParameters
            {
                ValidIssuer              = "licensehub.blitedb.com",
                ValidAudience            = "blite-server",
                IssuerSigningKey         = new RsaSecurityKey(rsa),
                ValidateIssuerSigningKey = true,
                ValidateIssuer           = true,
                ValidateAudience         = true,
                ValidateLifetime         = false, // we check expiry ourselves
                ClockSkew                = TimeSpan.Zero,
            };

            var principal = handler.ValidateToken(jwt, parameters, out var token);
            var jwtToken  = (JwtSecurityToken)token;

            info = new LicenseInfo(
                LicenseId:   jwtToken.Id,
                CustomerId:  principal.FindFirst("customer_id")?.Value ?? string.Empty,
                Tier:        principal.FindFirst("tier")?.Value         ?? "commercial",
                MaxInstances: int.TryParse(
                    principal.FindFirst("max_instances")?.Value, out var max) ? max : 1,
                IssuedAt:    jwtToken.IssuedAt,
                ExpiresAt:   jwtToken.ValidTo);
            return true;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
            return false;
        }
        finally
        {
            rsa?.Dispose();
        }
    }
}

public sealed record LicenseInfo(
    string   LicenseId,
    string   CustomerId,
    string   Tier,
    int      MaxInstances,
    DateTime IssuedAt,
    DateTime ExpiresAt);
