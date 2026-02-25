// BLite.Server — Query cache configuration
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.Caching;

public sealed class QueryCacheOptions
{
    public bool   Enabled                   { get; init; } = false;
    public int    SlidingExpirationSeconds  { get; init; } = 30;
    public int    AbsoluteExpirationSeconds { get; init; } = 300;
    public long   MaxSizeBytes              { get; init; } = 64 * 1024 * 1024; // 64 MB
    public int    MaxResultSetSize          { get; init; } = 500; // skip cache if result > N docs
}
