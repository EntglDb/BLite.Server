// BLite.Server — AdminService implementation
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All RPCs require the caller to hold BLiteOperation.Admin.
// The plaintext API key is returned once at creation/rotation and never stored.

using BLite.Core;
using BLite.Proto.V1;
using BLite.Server.Auth;
using Grpc.Core;

namespace BLite.Server.Services;

public sealed class AdminServiceImpl : AdminService.AdminServiceBase
{
    private readonly UserRepository     _users;
    private readonly AuthorizationService _authz;
    private readonly ILogger<AdminServiceImpl> _logger;

    public AdminServiceImpl(
        UserRepository users, AuthorizationService authz,
        ILogger<AdminServiceImpl> logger)
    {
        _users  = users;
        _authz  = authz;
        _logger = logger;
    }

    // ── CreateUser ────────────────────────────────────────────────────────────

    public override async Task<CreateUserResponse> CreateUser(
        CreateUserRequest request, ServerCallContext context)
    {
        RequireAdmin(context);
        try
        {
            var perms = MapPerms(request.Permissions);
            string? ns = string.IsNullOrWhiteSpace(request.Namespace) ? null : request.Namespace;
            var (_, plainKey) = await _users.CreateAsync(request.Username, ns, perms,
                                                         context.CancellationToken);
            _logger.LogInformation("Created user '{User}' (ns={Ns})", request.Username, ns ?? "(root)");
            return new CreateUserResponse { ApiKey = plainKey };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "CreateUser failed for '{User}'", request.Username);
            return new CreateUserResponse { Error = ex.Message };
        }
    }

    // ── RevokeUser ────────────────────────────────────────────────────────────

    public override async Task<MutationResponse> RevokeUser(
        UsernameRequest request, ServerCallContext context)
    {
        RequireAdmin(context);
        if (request.Username.Equals("root", StringComparison.OrdinalIgnoreCase))
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "The root user cannot be revoked."));

        var ok = await _users.RevokeAsync(request.Username, context.CancellationToken);
        _logger.LogInformation("RevokeUser '{User}' → {Result}", request.Username, ok);
        return new MutationResponse { Success = ok };
    }

    // ── RotateKey ─────────────────────────────────────────────────────────────

    public override async Task<RotateKeyResponse> RotateKey(
        UsernameRequest request, ServerCallContext context)
    {
        RequireAdmin(context);
        var plainKey = await _users.RotateKeyAsync(request.Username, context.CancellationToken);
        if (plainKey is null)
            return new RotateKeyResponse { Error = $"User '{request.Username}' not found." };

        _logger.LogInformation("Rotated key for '{User}'", request.Username);
        return new RotateKeyResponse { ApiKey = plainKey };
    }

    // ── ListUsers ─────────────────────────────────────────────────────────────

    public override Task<ListUsersResponse> ListUsers(
        Empty request, ServerCallContext context)
    {
        RequireAdmin(context);
        var response = new ListUsersResponse();
        foreach (var u in _users.ListAll())
        {
            var info = new UserInfo
            {
                Username   = u.Username,
                Namespace  = u.Namespace ?? "",
                Active     = u.Active,
                CreatedAt  = u.CreatedAt.ToString("O")
            };
            info.Permissions.AddRange(
                u.Permissions.Select(p => new UserPermission
                {
                    Collection = p.Collection,
                    Ops        = (int)p.Ops
                }));
            response.Users.Add(info);
        }
        return Task.FromResult(response);
    }

    // ── UpdatePerms ───────────────────────────────────────────────────────────

    public override async Task<MutationResponse> UpdatePerms(
        UpdatePermsRequest request, ServerCallContext context)
    {
        RequireAdmin(context);
        var perms = MapPerms(request.Permissions);
        var ok    = await _users.UpdatePermissionsAsync(request.Username, perms,
                                                       context.CancellationToken);
        return new MutationResponse { Success = ok };
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void RequireAdmin(ServerCallContext ctx)
    {
        var user = BLiteServiceBase.GetCurrentUser(ctx);
        if (!user.Permissions.Any(p =>
                (p.Collection == "*" || p.Collection == "_admin") &&
                (p.Ops & BLiteOperation.Admin) != 0))
        {
            throw new RpcException(new Status(
                StatusCode.PermissionDenied, "Admin permission required."));
        }
    }

    private static IReadOnlyList<PermissionEntry> MapPerms(
        IEnumerable<UserPermission> proto) =>
        proto.Select(p => new PermissionEntry(p.Collection, (BLiteOperation)p.Ops)).ToList();
}
