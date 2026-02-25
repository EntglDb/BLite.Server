// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks are enforced by PermissionFilter before handlers run.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using BLite.Server.Auth;

namespace BLite.Server.Rest;

/// <summary>
/// Provides extension methods for mapping user-related API endpoints in a RESTful service.
/// </summary>
/// <remarks>This class contains methods to handle user operations such as listing, creating, deleting, and
/// updating user permissions. It requires an authorization service to ensure that only authorized users can perform
/// these actions.</remarks>
internal static class RestApiUsersExtensions
{
    /// <summary>
    /// Configures user management API endpoints for listing, creating, deleting, and updating user permissions within
    /// the specified route group.
    /// </summary>
    /// <remarks>This method sets up endpoints for user operations, including GET /api/v1/users to retrieve
    /// all users, POST /api/v1/users to create a new user, DELETE /api/v1/users/{username} to remove a user, and PUT
    /// /api/v1/users/{username}/permissions to modify user permissions. Each endpoint requires administrative
    /// authorization and handles specific request and response formats. Use this method to integrate user management
    /// functionality into the application's API routing.</remarks>
    /// <param name="g">The RouteGroupBuilder used to define and organize the routing for user-related API endpoints.</param>
    internal static void MapUsers(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("Users")
                     .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, "*"));

        // GET /api/v1/users — list all users
        group.MapGet("/users",
            (UserRepository users) =>
            {
                var list = users.ListAll().Select(u => new
                {
                    u.Username,
                    u.Namespace,
                    u.DatabaseId,
                    u.Active,
                    CreatedAt = u.CreatedAt.ToString("O"),
                    Permissions = u.Permissions.Select(p => new { p.Collection, Ops = p.Ops.ToString() })
                });
                return Results.Ok(list);
            })
            .WithSummary("List users")
            .WithDescription("Returns all registered users together with their namespaces, database restrictions, active status, and permission grants.");

        // POST /api/v1/users — create a user
        //   Body: { "username": "alice", "namespace": null, "databaseId": null }
        group.MapPost("/users",
            async (UserRepository users,
                   CreateUserRequest req,
                   CancellationToken ct) =>
            {
                if (string.IsNullOrWhiteSpace(req.Username))
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["username"] = ["username is required."]
                    });

                try
                {
                    var perms = new List<PermissionEntry>
                    {
                        new("*", BLiteOperation.All)
                    };
                    var ns = string.IsNullOrWhiteSpace(req.Namespace) ? null : req.Namespace.Trim();
                    var dbId = string.IsNullOrWhiteSpace(req.DatabaseId) ? null : req.DatabaseId.Trim();
                    var (_, plainKey) = await users.CreateAsync(req.Username.Trim(), ns, perms, dbId, ct);
                    return Results.Created($"/api/v1/users/{req.Username.Trim()}",
                        new { username = req.Username.Trim(), apiKey = plainKey });
                }
                catch (InvalidOperationException)
                {
                    return BLiteErrors.UserAlreadyExists(req.Username).ToResult();
                }
            })
            .WithSummary("Create a user")
            .WithDescription("Creates a new user and generates an API key. The plaintext key is returned only in this response and cannot be retrieved again. The user is created with full (`*`) permissions by default.");

        // DELETE /api/v1/users/{username}
        group.MapDelete("/users/{username}",
            async (UserRepository users,
                   string username,
                   CancellationToken ct) =>
            {
                var deleted = await users.DeleteUserAsync(username, ct);
                return deleted
                    ? Results.NoContent()
                    : BLiteErrors.UserNotFound(username).ToResult();
            })
            .WithSummary("Delete a user")
            .WithDescription("Permanently removes the user and revokes their API key. Returns 404 if the user does not exist.");

        // PUT /api/v1/users/{username}/permissions
        //   Body: [{ "collection": "*", "ops": 63 }, ...]
        group.MapPut("/users/{username}/permissions",
            async (UserRepository users,
                   string username,
                   List<PermissionRequest> perms,
                   CancellationToken ct) =>
            {
                var entries = perms
                    .Where(p => !string.IsNullOrWhiteSpace(p.Collection))
                    .Select(p => new PermissionEntry(p.Collection.Trim(), (BLiteOperation)p.Ops))
                    .ToList();

                var ok = await users.UpdatePermissionsAsync(username, entries, ct);
                return ok
                    ? Results.NoContent()
                    : BLiteErrors.UserNotFound(username).ToResult();
            })
            .WithSummary("Replace user permissions")
            .WithDescription("Replaces the full permission set for a user. Each entry specifies a collection name (or `*` for all collections) and a bitmask of allowed operations.");
    }
}
