// BLite.Server — ErrorOr error catalogue + IResult mapping helpers
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Server.Auth;
using ErrorOr;

namespace BLite.Server.Rest;

/// <summary>
/// Factory methods for common REST API errors, modelled as <see cref="Error"/>
/// values from the <see href="https://github.com/amantinband/error-or">ErrorOr</see> library.
/// </summary>
public static class BLiteErrors
{
    // ── Auth ─────────────────────────────────────────────────────────────────

    public static Error MissingKey()
        => Error.Unauthorized(
            code:        "Auth.MissingKey",
            description: "A valid x-api-key header (or Authorization: Bearer <key>) is required.");

    public static Error PermissionDenied(string username, BLiteOperation op, string collection)
        => Error.Forbidden(
            code:        "Auth.PermissionDenied",
            description: $"User '{username}' lacks {op} permission on collection '{collection}'.");

    public static Error InactiveUser(string username)
        => Error.Forbidden(
            code:        "Auth.InactiveUser",
            description: $"User '{username}' is revoked and cannot perform operations.");

    // ── Resources ────────────────────────────────────────────────────────────

    public static Error DatabaseNotFound(string databaseId)
        => Error.NotFound(
            code:        "Database.NotFound",
            description: $"Database '{databaseId}' does not exist.");

    public static Error DatabaseAlreadyExists(string databaseId)
        => Error.Conflict(
            code:        "Database.AlreadyExists",
            description: $"Database '{databaseId}' already exists.");

    public static Error CollectionNotFound(string collection)
        => Error.NotFound(
            code:        "Collection.NotFound",
            description: $"Collection '{collection}' does not exist.");

    public static Error DocumentNotFound(string id)
        => Error.NotFound(
            code:        "Document.NotFound",
            description: $"Document '{id}' was not found.");

    public static Error UserNotFound(string username)
        => Error.NotFound(
            code:        "User.NotFound",
            description: $"User '{username}' was not found.");

    public static Error UserAlreadyExists(string username)
        => Error.Conflict(
            code:        "User.AlreadyExists",
            description: $"A user with username '{username}' already exists.");

    // ── Input ────────────────────────────────────────────────────────────────

    public static Error InvalidJson(string detail)
        => Error.Validation(
            code:        "Input.InvalidJson",
            description: detail);

    public static Error InvalidId(string id)
        => Error.Validation(
            code:        "Input.InvalidId",
            description: $"'{id}' is not a valid BsonId.");

    // ── IResult mapping ──────────────────────────────────────────────────────

    /// <summary>Maps a raw <see cref="Error"/> directly to an <see cref="IResult"/>.</summary>
    public static IResult ToResult(this Error error) => MapError(error);

    /// <summary>
    /// Maps an <see cref="ErrorOr{T}"/> to an <see cref="IResult"/>, using
    /// <paramref name="onSuccess"/> for the happy path.
    /// </summary>
    public static IResult ToResult<T>(this ErrorOr<T> result, Func<T, IResult> onSuccess)
        => result.IsError ? MapError(result.FirstError) : onSuccess(result.Value);

    /// <summary>Maps an <see cref="ErrorOr{Deleted}"/> (void-like) to an <see cref="IResult"/>.</summary>
    public static IResult ToResult(this ErrorOr<Deleted> result)
        => result.IsError ? MapError(result.FirstError) : Results.NoContent();

    private static IResult MapError(Error error) => error.Type switch
    {
        ErrorType.Unauthorized => Results.Problem(
            title:      "Unauthorized",
            detail:     error.Description,
            statusCode: StatusCodes.Status401Unauthorized),

        ErrorType.Forbidden => Results.Problem(
            title:      "Forbidden",
            detail:     error.Description,
            statusCode: StatusCodes.Status403Forbidden),

        ErrorType.NotFound => Results.Problem(
            title:      "Not Found",
            detail:     error.Description,
            statusCode: StatusCodes.Status404NotFound),

        ErrorType.Conflict => Results.Problem(
            title:      "Conflict",
            detail:     error.Description,
            statusCode: StatusCodes.Status409Conflict),

        ErrorType.Validation => Results.ValidationProblem(
            errors: new Dictionary<string, string[]>
            {
                [error.Code] = [error.Description]
            }),

        _ => Results.Problem(
            title:      "Internal Server Error",
            detail:     error.Description,
            statusCode: StatusCodes.Status500InternalServerError),
    };
}
