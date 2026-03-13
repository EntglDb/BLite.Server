// BLite.Client.IntegrationTests — WatchTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end integration tests for the Watch / CDC gRPC streaming feature.
// Tests exercise RemoteDocumentCollection<int, TestProduct>.Watch(), which routes
// through DynamicService.Watch (server-side streaming RPC) and bridges the
// resulting ChangeEventResponse stream into IObservable<ChangeStreamEvent<TId,T>>.

using BLite.Client;
using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;
using BLite.Client.IntegrationTests.Infrastructure.Mappers;
using BLite.Core.CDC;
using BLite.Core.Collections;
using BLite.Core.Transactions;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class WatchTests : IntegrationTestBase
{
    public WatchTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Mapper helper ─────────────────────────────────────────────────────────

    private sealed class NamedProductMapper(string collectionName)
        : BLite_Client_IntegrationTests_Infrastructure_TestProductMapper
    {
        public override string CollectionName => collectionName;
    }

    // ── Setup ─────────────────────────────────────────────────────────────────

    private (BLiteClient Client, IDocumentCollection<int, TestProduct> Col) Setup()
    {
        var client = CreateClient();
        var col = client.GetDocumentCollection<int, TestProduct>(
            new NamedProductMapper(UniqueCollection()));
        return (client, col);
    }

    private static TestProduct NewProduct(int id, string name, decimal price = 9.99m, int stock = 10)
        => new() { Id = id, Name = name, Price = price, Stock = stock };

    // ── Event collection helper ───────────────────────────────────────────────

    /// <summary>
    /// Subscribes, waits 200 ms for the gRPC Watch stream to establish, then
    /// runs <paramref name="trigger"/> and waits until <paramref name="expectedCount"/>
    /// events arrive or the timeout elapses. Disposes the subscription before returning.
    /// </summary>
    private static async Task<List<ChangeStreamEvent<int, TestProduct>>> CollectAsync(
        IObservable<ChangeStreamEvent<int, TestProduct>> observable,
        int expectedCount,
        Func<Task> trigger,
        int timeoutMs = 5000)
    {
        var events = new List<ChangeStreamEvent<int, TestProduct>>();
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = observable.Subscribe(e =>
        {
            lock (events)
            {
                events.Add(e);
                if (events.Count >= expectedCount)
                    tcs.TrySetResult();
            }
        });

        // Give the background ConsumeAsync task time to open the gRPC stream and
        // have the server install its Watch subscription before mutations fire.
        await Task.Delay(200);

        await trigger();
        await Task.WhenAny(tcs.Task, Task.Delay(timeoutMs));
        return events;
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Insert_EmitsInsertEvent()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.InsertAsync(NewProduct(1, "Widget")));

        Assert.Single(events);
        Assert.Equal(OperationType.Insert, events[0].Type);
    }

    [Fact]
    public async Task Watch_Insert_DocumentId_MatchesInsertedId()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.InsertAsync(NewProduct(42, "Gadget")));

        Assert.Single(events);
        Assert.Equal(42, events[0].DocumentId);
    }

    // ── Update ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Update_EmitsUpdateEvent()
    {
        var (client, col) = Setup();
        await using var _ = client;

        await col.InsertAsync(NewProduct(1, "Original"));

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.UpdateAsync(NewProduct(1, "Updated")));

        Assert.Single(events);
        Assert.Equal(OperationType.Update, events[0].Type);
        Assert.Equal(1, events[0].DocumentId);
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Delete_EmitsDeleteEvent()
    {
        var (client, col) = Setup();
        await using var _ = client;

        await col.InsertAsync(NewProduct(1, "ToDelete"));

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.DeleteAsync(1));

        Assert.Single(events);
        Assert.Equal(OperationType.Delete, events[0].Type);
        Assert.Equal(1, events[0].DocumentId);
    }

    // ── Payload capture ───────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_CapturePayloadTrue_EntityIsPopulated()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: true),
            expectedCount: 1,
            trigger: () => col.InsertAsync(NewProduct(7, "Payload", 19.99m, stock: 5)));

        Assert.Single(events);
        Assert.NotNull(events[0].Entity);
        Assert.Equal("Payload", events[0].Entity!.Name);
        Assert.Equal(19.99m,    events[0].Entity.Price);
        Assert.Equal(5,         events[0].Entity.Stock);
    }

    [Fact]
    public async Task Watch_CapturePayloadFalse_EntityIsNull()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.InsertAsync(NewProduct(8, "NoPayload")));

        Assert.Single(events);
        Assert.Null(events[0].Entity);
    }

    // ── Multiple events ───────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_MultipleInserts_AllEventsReceived()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 3,
            trigger: async () =>
            {
                await col.InsertAsync(NewProduct(1, "A"));
                await col.InsertAsync(NewProduct(2, "B"));
                await col.InsertAsync(NewProduct(3, "C"));
            });

        Assert.Equal(3, events.Count);
        Assert.All(events, e => Assert.Equal(OperationType.Insert, e.Type));
    }

    [Fact]
    public async Task Watch_InsertUpdateDelete_CorrectEventSequence()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 3,
            trigger: async () =>
            {
                await col.InsertAsync(NewProduct(99, "Seq"));
                await col.UpdateAsync(NewProduct(99, "SeqUpdated"));
                await col.DeleteAsync(99);
            });

        Assert.Equal(3, events.Count);
        Assert.Equal(OperationType.Insert, events[0].Type);
        Assert.Equal(OperationType.Update, events[1].Type);
        Assert.Equal(OperationType.Delete, events[2].Type);
        Assert.All(events, e => Assert.Equal(99, e.DocumentId));
    }

    // ── CollectionName ────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Event_CollectionNameMatchesTarget()
    {
        var client = CreateClient();
        await using var _ = client;

        var colName = UniqueCollection();
        var col = client.GetDocumentCollection<int, TestProduct>(new NamedProductMapper(colName));

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.InsertAsync(NewProduct(1, "ColCheck")));

        Assert.Single(events);
        Assert.Equal(colName, events[0].CollectionName);
    }

    // ── Timestamp ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Event_TimestampIsPositive()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = await CollectAsync(
            col.Watch(capturePayload: false),
            expectedCount: 1,
            trigger: () => col.InsertAsync(NewProduct(1, "Ts")));

        Assert.Single(events);
        Assert.True(events[0].Timestamp > 0);
    }

    // ── Dispose stops stream ──────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Dispose_StopsReceivingEvents()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var events = new List<ChangeStreamEvent<int, TestProduct>>();
        var firstEventReceived =
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var sub = col.Watch(capturePayload: false).Subscribe(e =>
        {
            lock (events)
            {
                events.Add(e);
                firstEventReceived.TrySetResult();
            }
        });

        await Task.Delay(200); // allow Watch stream to establish

        await col.InsertAsync(NewProduct(1, "Before"));
        await Task.WhenAny(firstEventReceived.Task, Task.Delay(5000));

        sub.Dispose();
        await Task.Delay(300); // allow cancellation to propagate

        var countAfterDispose = events.Count;

        await col.InsertAsync(NewProduct(2, "After"));
        await Task.Delay(500); // give any late events a chance to arrive

        Assert.Equal(countAfterDispose, events.Count);
    }
}
