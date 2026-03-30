// BLite.Server.Benchmarks — entry point
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Usage:
//   dotnet run -c Release                    — interactive benchmark menu
//   dotnet run -c Release -- --filter *      — run all benchmarks
//   dotnet run -c Release -- --filter *Crud* — run CrudBenchmarks only

using System.Net.Sockets;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Running;
using BLite.Server.Benchmarks;

var mongoUrl = Environment.GetEnvironmentVariable("MONGO_URL") ?? "mongodb://localhost:27017";
var mongoAvailable = await IsTcpReachableAsync(mongoUrl);

if (!mongoAvailable)
    Console.WriteLine("[Benchmarks] MongoDB not reachable — Mongo benchmarks excluded.");

var config = mongoAvailable
    ? (IConfig)DefaultConfig.Instance
    : ManualConfig.Create(DefaultConfig.Instance)
          .AddFilter(new SimpleFilter(b =>
          {
              var name = b.Descriptor.WorkloadMethod.Name;
              return !name.EndsWith("_Mongo") && !name.EndsWith("_MongoJ");
          }));

BenchmarkSwitcher.FromAssembly(typeof(CrudBenchmarks).Assembly).Run(args, config);

static async Task<bool> IsTcpReachableAsync(string url, int timeoutMs = 1500)
{
    try
    {
        var uri  = new Uri(url);
        var host = uri.Host;
        var port = uri.Port > 0 ? uri.Port : 27017;
        using var tc  = new TcpClient();
        using var cts = new CancellationTokenSource(timeoutMs);
        await tc.ConnectAsync(host, port, cts.Token);
        return true;
    }
    catch { return false; }
}
