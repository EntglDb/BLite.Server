// BLite.Server — API Key authentication
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.Auth;

/// <summary>
/// Validates incoming API keys against the configured allow-list.
/// If no keys are configured the server runs in open/dev mode (all requests accepted).
/// </summary>
public sealed class ApiKeyValidator
{
    private readonly HashSet<string> _keys;

    public ApiKeyValidator(IEnumerable<string> configuredKeys)
    {
        _keys = new HashSet<string>(configuredKeys, StringComparer.Ordinal);
    }

    /// <summary>Returns <c>true</c> when auth is disabled or the key is valid.</summary>
    public bool IsValid(string? key)
        => _keys.Count == 0 || (_keys.Count > 0 && key is not null && _keys.Contains(key));
}
