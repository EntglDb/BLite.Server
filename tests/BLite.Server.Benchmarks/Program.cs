// BLite.Server.Benchmarks — entry point
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Usage:
//   dotnet run -c Release                    — interactive benchmark menu
//   dotnet run -c Release -- --filter *      — run all benchmarks
//   dotnet run -c Release -- --filter *Crud* — run CrudBenchmarks only

using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
