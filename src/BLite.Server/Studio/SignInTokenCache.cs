// BLite.Server — One-time sign-in token cache
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Bridges the Blazor Server interactive circuit (no HttpContext)
// with the ASP.NET Core cookie sign-in (needs HttpContext).
// Flow: Blazor validates the API key → stores a short-lived token here →
//       redirects to /studio/sign-in?t={token} (a real HTTP request) →
//       minimal-API endpoint consumes the token and issues the auth cookie.

using System.Collections.Concurrent;

namespace BLite.Server.Studio;

/// <summary>
/// Thread-safe, expiring store of one-time sign-in tokens.
/// Each token is valid for <see cref="TokenLifetime"/> and can be consumed only once.
/// </summary>
public sealed class SignInTokenCache
{
    private static readonly TimeSpan TokenLifetime = TimeSpan.FromMinutes(5);

    private sealed record Entry(string Username, DateTimeOffset ExpiresAt);

    private readonly ConcurrentDictionary<string, Entry> _tokens = new();

    /// <summary>Creates a new one-time token that maps to the given username.</summary>
    public string Issue(string username)
    {
        Purge();
        var token = Guid.NewGuid().ToString("N");
        _tokens[token] = new Entry(username, DateTimeOffset.UtcNow + TokenLifetime);
        return token;
    }

    /// <summary>
    /// Validates and removes the token in a single atomic operation.
    /// Returns the username if valid, <c>null</c> otherwise.
    /// </summary>
    public string? Consume(string token)
    {
        if (!_tokens.TryRemove(token, out var entry)) return null;
        if (entry.ExpiresAt < DateTimeOffset.UtcNow)  return null;
        return entry.Username;
    }

    private void Purge()
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var (key, entry) in _tokens)
            if (entry.ExpiresAt < now) _tokens.TryRemove(key, out _);
    }
}
