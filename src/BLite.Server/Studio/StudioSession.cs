// BLite.Server — Studio session
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Thin adapter over ASP.NET Core cookie authentication.
// Authentication state is persisted in an HttpOnly cookie; the Blazor Server
// circuit receives the principal at startup via AuthenticationStateProvider.

using Microsoft.AspNetCore.Components.Authorization;

namespace BLite.Server.Studio;

/// <summary>
/// Exposes the current Studio user's authentication state to Blazor components.
/// Backed by <see cref="AuthenticationStateProvider"/> so the state survives
/// page navigations and circuit reconnections.
/// </summary>
public sealed class StudioSession(AuthenticationStateProvider authStateProvider)
{
    private readonly AuthenticationStateProvider _asp = authStateProvider;

    /// <summary>Resolves whether the current user is authenticated.</summary>
    public async Task<bool> IsAuthenticatedAsync()
    {
        var state = await _asp.GetAuthenticationStateAsync();
        return state.User.Identity?.IsAuthenticated == true;
    }

    /// <summary>Returns the authenticated username, or null.</summary>
    public async Task<string?> GetUsernameAsync()
    {
        var state = await _asp.GetAuthenticationStateAsync();
        return state.User.Identity?.IsAuthenticated == true
            ? state.User.Identity.Name
            : null;
    }
}
