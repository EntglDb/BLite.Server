# Embedding Worker — Piano di Implementazione

> **Stato:** Design approvato — pronto per implementazione  
> **Data:** 2026  
> **Autore:** Luca Fabbri

---

## Contesto

BLite.Server deve essere in grado di vettorializzare automaticamente i documenti
di una collection ogni volta che vengono inseriti o modificati, purché:

1. La collection abbia una **VectorSource** configurata (campi da concatenare)
2. La collection abbia almeno un **indice Vector (HNSW)** definito

Il testo viene costruito da `TextNormalizer.BuildEmbeddingText()` e passato a
`EmbeddingService.Embed()` (ONNX / MiniLM-L6-v2).

---

## Approccio scelto: Coda persistente + CDC nativo

### Perché non il polling cieco

Il "worker ignorante" (scansione periodica di tutti i documenti) ha costi e difetti:

- Deve leggere ogni documento per decidere se è già vettorializzato
- Non sa quali documenti sono stati modificati tra un ciclo e l'altro
- Hash dei campi sorgente → doppia computazione ad ogni ciclo
- Nessuna separazione tra "cosa devo fare" e "quando lo faccio"

### Perché la coda persistente

- Ogni documento modificato produce **un solo task** nella coda
- Il worker consuma esattamente ciò che è cambiato — zero sprechi
- La coda sopravvive ai riavvii (persistita nel system database)
- Deduplicazione integrata: se lo stesso documento viene modificato N volte
  prima che il worker lo processi, rimane **un solo task** in coda

### Perché il CDC nativo di BLite

BLite.Core ha già `ChangeStreamDispatcher` in `BLite.Core.CDC`:

```csharp
// Transaction.CommitAsync() — dopo il WAL flush fisico
if (_pendingChanges.Count > 0 && _storage.Cdc != null)
    foreach (var change in _pendingChanges)
        _storage.Cdc.Publish(change);  // fired post-commit, sempre
```

Questo garantisce:

| Proprietà | Valore |
|---|---|
| Timing | **Post-commit** — impossibile ricevere eventi da transazioni rollbackate |
| Copertura | **Tutti i path di scrittura** (gRPC, REST, Studio, engine diretto, bulk) |
| Modifiche ai siti di scrittura | **Zero** — nessun handler da toccare |
| Overhead quando non usato | **Zero** — dispatcher lazy-init |

---

## Architettura

```
┌─────────────────────────────────────────────────────────────────┐
│  Qualsiasi write path (gRPC / REST / Studio / engine diretto)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ InsertAsync / UpdateAsync / CommitAsync
                           ▼
              ┌────────────────────────┐
              │  Transaction.CommitAsync│  ← WAL flush fisico
              │  + CDC Publish()       │  ← post-commit
              └────────────┬───────────┘
                           │ InternalChangeEvent
                           ▼
           ┌───────────────────────────────┐
           │   ChangeStreamDispatcher      │  BLite.Core.CDC
           │   Channel<InternalChangeEvent>│  (già esistente)
           └───────────────┬───────────────┘
                           │ fan-out per collection
                           ▼
         ┌──────────────────────────────────────┐
         │   EmbeddingQueuePopulator            │  IHostedService
         │   - filtra Insert + Update           │
         │   - scarta Delete                    │
         │   - verifica VectorSource+VectorIndex│
         └──────────────────────┬───────────────┘
                                │ Enqueue(dbId, col, docId)
                                ▼
                    ┌──────────────────────┐
                    │   EmbeddingQueue     │  Singleton service
                    │   _emb_queue         │  collection nel system db
                    │   - dedup            │
                    │   - stato persistito │
                    └──────────┬───────────┘
                               │ TakeBatch(n) → in_progress
                               ▼
              ┌───────────────────────────────────┐
              │   EmbeddingWorker                 │  BackgroundService
              │   - sveglia ogni IntervalSeconds  │
              │   - raggruppa task per database   │
              │   - Embed() fuori dalla tx        │
              │   - unica tx per database         │
              │   - Complete(ids) → done          │
              └───────────────────────────────────┘
```

---

## Modello del task: `EmbeddingTask`

### Documento in `_emb_queue` (system db)

```json
{
  "_id":         "ObjectId(...)",
  "key":         "tenantdb:articles:abc123",
  "db":          "tenantdb",
  "col":         "articles",
  "doc":         "abc123",
  "enqueuedAt":  "2026-02-25T10:00:00Z",
  "changedAt":   "2026-02-25T10:00:00Z",
  "rawStatus":   "todo"
}
```

Il campo `key` (`{db}:{col}:{doc}`) è indicizzato con BTree per la deduplicazione O(log n).

### Status computato

```csharp
public enum EmbeddingTaskStatus { Todo, InProgress, Stale, Done }

public EmbeddingTaskStatus Status => _rawStatus switch
{
    "todo"        => EmbeddingTaskStatus.Todo,
    "done"        => EmbeddingTaskStatus.Done,
    "in_progress" => (DateTime.UtcNow - ChangedAt) > StaleThreshold
                        ? EmbeddingTaskStatus.Stale
                        : EmbeddingTaskStatus.InProgress,
    _             => EmbeddingTaskStatus.Todo
};
```

`"stale"` non viene mai scritto su disco — è un valore derivato dallo scorrere del tempo.
Il timeout è configurabile (default: 5 minuti).

### Ciclo di vita di un task

```
                    ┌──── nuovo Insert/Update sullo stesso doc
                    │     se task in todo/in_progress:
                    │     DELETE old → INSERT nuovo todo
                    │
[evento CDC] ──► todo ──► in_progress ──► done ──► [eliminato dopo retention]
                               │
                               └──► stale (se timeout scade)
                                        │
                               [prossimo ciclo worker] ──► in_progress
```

---

## Logica di `Enqueue` (dedup)

```
Enqueue(db, col, docId):
  key = "{db}:{col}:{docId}"
  existing = _emb_queue.FindByIndex("key", key) WHERE rawStatus != "done"
  if existing != null:
    DELETE existing        ← elimina vecchio todo o in_progress
  INSERT { key, db, col, doc, enqueuedAt=now, changedAt=now, rawStatus="todo" }
```

Se un documento viene modificato 100 volte prima che il worker si svegli,
nella coda esiste **esattamente un task** — l'ultimo.

---

## Ciclo del worker

```
ogni IntervalSeconds:
  tasks = queue.TakeBatch(BatchSize)
    → SELECT todo + stale ORDER BY enqueuedAt LIMIT n
    → UPDATE rawStatus="in_progress", changedAt=now
  
  if tasks.Count == 0: return
  
  byDb = tasks.GroupBy(t => t.Database)
  
  per ogni gruppo (database):
    engine = registry.GetEngine(dbId)
    
    # Fase 1: embedding (CPU-bound, FUORI dalla transazione)
    toUpdate = []
    per ogni task nel gruppo:
      config = engine.GetVectorSource(task.Col)
      vecIdx = engine.GetIndexDescriptors(task.Col).First(Vector)
      if config == null || vecIdx == null:
        completed.Add(task.Id)   # niente da fare
        continue
      doc = engine.FindById(task.Col, docId)
      if doc == null:
        completed.Add(task.Id)   # documento eliminato nel frattempo
        continue
      text = TextNormalizer.BuildEmbeddingText(doc, config)
      if text == "": completed.Add(task.Id); continue
      vector = embedding.Embed(text)
      toUpdate.Add((task, vecIdx.FieldPath, vector))
    
    # Fase 2: persistenza — unica transazione per database
    engine.BeginTransaction()
    try:
      per ogni (task, fieldPath, vector) in toUpdate:
        doc = engine.FindById(task.Col, docId)
        updatedDoc = CopyDocumentWithVector(engine, doc, fieldPath, vector)
        engine.GetOrCreateCollection(task.Col).Update(docId, updatedDoc)
        completed.Add(task.Id)
      engine.Commit()
    catch:
      engine.Rollback()
      raise
  
  queue.Complete(completed)
    → UPDATE rawStatus="done", changedAt=now WHERE _id IN (...)
```

### Costruzione del documento aggiornato

```csharp
// BsonDocumentBuilder.Add(name, BsonValue) e AddFloatArray(name, float[]) esistono già.
// Copia tutti i campi esistenti + sovrascrive/aggiunge fieldPath con il vettore.

engine.RegisterKeys([fieldPath]);
var builder = new BsonDocumentBuilder(engine.GetKeyMap(), engine.GetKeyReverseMap());
foreach (var (name, value) in doc.EnumerateFields())
{
    if (name != fieldPath)
        builder.Add(name, value);
}
builder.AddFloatArray(fieldPath, vector);
return builder.Build();
```

---

## `EmbeddingQueuePopulator`

Gestisce le sottoscrizioni CDC per le collection rilevanti.

### Startup

```
StartAsync():
  per ogni (dbId, engine) in registry.GetAllActiveEngines():
    per ogni colName in engine.ListCollections():
      if engine.GetVectorSource(colName) != null
         && engine.GetIndexDescriptors(colName).Any(Vector):
        SubscribeCollection(dbId, engine, colName)
```

### Sottoscrizione

```csharp
void SubscribeCollection(string? dbId, BLiteEngine engine, string collection)
{
    var key = $"{dbId ?? ""}:{collection}";
    if (_subscriptions.ContainsKey(key)) return;

    var channel = Channel.CreateUnbounded<(OperationType, BsonId)>();
    var sub = engine.SubscribeToChanges(collection, channel.Writer);
    _subscriptions[key] = sub;

    // pump asincrono — per ogni evento CDC → Enqueue
    _ = Task.Run(async () =>
    {
        await foreach (var (op, id) in channel.Reader.ReadAllAsync(_cts.Token))
        {
            if (op is OperationType.Insert or OperationType.Update)
                _queue.Enqueue(dbId, collection, id);
        }
    }, _cts.Token);
}
```

### Aggiornamento dinamico delle sottoscrizioni

`StudioService.SetVectorSource()` e la REST API chiamano:

```csharp
_populator.RefreshSubscription(dbId, collection);
// → subscribe se ora ha VectorSource+VectorIndex, unsubscribe se rimossa
```

---

## Modifiche a `BLite.Core`

### `BLiteEngine.cs` — aggiungere API pubblica CDC

```csharp
/// <summary>
/// Subscribes to post-commit CDC events for <paramref name="collectionName"/>.
/// Only Insert and Update events are forwarded (Delete is filtered out).
/// The <paramref name="writer"/> receives (OperationType, BsonId) pairs.
/// Returns an <see cref="IDisposable"/> that unsubscribes when disposed.
/// </summary>
public IDisposable SubscribeToChanges(
    string collectionName,
    ChannelWriter<(OperationType Op, BsonId Id)> writer)
{
    ThrowIfDisposed();
    _storage.EnsureCdc();
    // Adapter channel: InternalChangeEvent → (OperationType, BsonId)
    var adapterChannel = Channel.CreateUnbounded<InternalChangeEvent>();
    var subscription = _storage.Cdc!.Subscribe(
        collectionName, capturePayload: false, adapterChannel.Writer);

    Task.Run(async () =>
    {
        await foreach (var e in adapterChannel.Reader.ReadAllAsync())
        {
            if (e.Type is OperationType.Insert or OperationType.Update)
            {
                var id = BsonId.FromBytes(e.IdBytes.Span);
                writer.TryWrite((e.Type, id));
            }
        }
    });

    return subscription;
}
```

> **Da verificare:** esistenza di `BsonId.FromBytes(ReadOnlySpan<byte>)` e
> `StorageEngine.EnsureCdc()` (metodo che inizializza il dispatcher se null).

---

## File da creare / modificare

### `BLite.Core` (sibling repo)

| File | Tipo | Nota |
|---|---|---|
| `BLiteEngine.cs` | MODIFY | Aggiunge `SubscribeToChanges()` |
| `Storage/StorageEngine.cs` | MODIFY | Aggiunge `EnsureCdc()` se non esiste |

### `BLite.Server`

| File | Tipo | Contenuto |
|---|---|---|
| `Embedding/EmbeddingTaskStatus.cs` | CREATE | `enum { Todo, InProgress, Stale, Done }` |
| `Embedding/EmbeddingTask.cs` | CREATE | Record con `Status` getter (stale-aware) |
| `Embedding/IEmbeddingQueue.cs` | CREATE | `Enqueue / TakeBatch / Complete / GetStats` |
| `Embedding/EmbeddingQueue.cs` | CREATE | Impl BLite-backed su `_emb_queue` |
| `Embedding/EmbeddingQueuePopulator.cs` | CREATE | `IHostedService` — subscribe CDC → Enqueue |
| `Embedding/EmbeddingWorkerOptions.cs` | CREATE | `Enabled / IntervalSeconds / BatchSize / StaleTimeoutMinutes / RetentionHours` |
| `Embedding/EmbeddingWorker.cs` | CREATE | `BackgroundService` — TakeBatch → Embed → Tx → Complete |
| `EngineRegistry.cs` | MODIFY | Aggiunge `GetAllActiveEngines()` |
| `Program.cs` | MODIFY | Registra `EmbeddingQueue`, `EmbeddingQueuePopulator`, `EmbeddingWorker` |
| `appsettings.json` | MODIFY | Sezione `EmbeddingWorker` |

---

## Configurazione (`appsettings.json`)

```json
"EmbeddingWorker": {
  "Enabled": true,
  "IntervalSeconds": 60,
  "BatchSize": 50,
  "StaleTimeoutMinutes": 5,
  "RetentionHours": 24
}
```

| Chiave | Default | Descrizione |
|---|---|---|
| `Enabled` | `true` | Abilita/disabilita il worker |
| `IntervalSeconds` | `60` | Secondi tra un ciclo e il successivo |
| `BatchSize` | `50` | Max documenti per ciclo per database |
| `StaleTimeoutMinutes` | `5` | Oltre questo tempo `in_progress` → `stale` |
| `RetentionHours` | `24` | Ore prima che i task `done` vengano eliminati |

---

## Registrazione DI (`Program.cs`)

```csharp
// dopo la registrazione di EmbeddingService
builder.Services.Configure<EmbeddingWorkerOptions>(
    builder.Configuration.GetSection(EmbeddingWorkerOptions.Section));

builder.Services.AddSingleton<IEmbeddingQueue, EmbeddingQueue>();
builder.Services.AddHostedService<EmbeddingQueuePopulator>();
builder.Services.AddHostedService<EmbeddingWorker>();
```

`EmbeddingQueue` è **Singleton** perché è condiviso tra il Populator (scrittore)
e il Worker (lettore). Entrambi i `HostedService` la ricevono via DI.

---

## Sequenza temporale: insert di un documento

```
t=0   Client chiama Insert("articles", doc)
t=1   BLiteEngine.InsertAsync() → WAL flush
t=2   Transaction.CommitAsync() → CDC Publish(InternalChangeEvent)
t=3   ChangeStreamDispatcher fan-out → adapterChannel di EmbeddingQueuePopulator
t=4   Pump asincrono → EmbeddingQueue.Enqueue("tenantdb", "articles", docId)
      _emb_queue: { rawStatus: "todo", enqueuedAt: t=4, key: "tenantdb:articles:xyz" }

t=60  EmbeddingWorker si sveglia
t=61  TakeBatch(50) → task trovato → rawStatus="in_progress"
t=62  TextNormalizer.BuildEmbeddingText(doc, config)
t=63  EmbeddingService.Embed(text) → float[384]    ← CPU/ONNX, fuori dalla tx
t=64  engine.BeginTransaction()
t=65  col.Update(docId, docConVettore)
t=66  engine.Commit()
t=67  queue.Complete([taskId]) → rawStatus="done"
```

---

## Gestione errori

| Scenario | Comportamento |
|---|---|
| `Embed()` lancia eccezione | Skip del documento, log Warning, task rimane `in_progress` → diventa `stale` al prossimo ciclo |
| `engine.Commit()` fallisce | Rollback, nessun `Complete()` → tutti i task del batch rimangono `in_progress` → diventano `stale` |
| Documento eliminato tra Enqueue e processing | `FindById()` restituisce null → task marcato `done` silenziosamente |
| VectorSource rimossa dalla collection | Task processato: `GetVectorSource()` = null → task marcato `done` senza embed |
| Processo crashato con task `in_progress` | Al riavvio, dopo `StaleTimeoutMinutes` passano a `stale` → riprocessati |

---

## Questioni aperte

| # | Domanda | Decisione consigliata |
|---|---|---|
| 1 | `BsonId.FromBytes(ReadOnlySpan<byte>)` esiste? | Verificare; se assente aggiungere a `BsonId` |
| 2 | `StorageEngine.EnsureCdc()` esiste? | Verificare; se assente aggiungere metodo che inizializza `_cdc` lazily |
| 3 | New tenant provisionato **dopo** startup | `EngineRegistry.ProvisionAsync()` deve notificare `EmbeddingQueuePopulator` tramite callback/event |
| 4 | Collection con **più VectorIndex** | Per ora: primo Vector index trovato. Futuro: VectorSourceConfig referenzia esplicitamente l'index name |
| 5 | Cleanup dei `done` | Il Worker alla fine di ogni ciclo elimina i task `done` con `changedAt < now - RetentionHours` |
| 6 | Studio UI per la coda | La collection `_emb_queue` è già visibile in Studio (collection riservata `_*`) — valutare nasconderla o mostrare una UI dedicata |

---

## Dipendenze tra componenti

```
EmbeddingQueuePopulator
  ├── IEmbeddingQueue          (scrittore)
  ├── EngineRegistry           (discovery engines + sottoscrizione CDC)
  └── EmbeddingWorkerOptions   (StaleTimeout per eventuale logica)

EmbeddingWorker
  ├── IEmbeddingQueue          (lettore)
  ├── EngineRegistry           (accesso ai tenant engines)
  ├── EmbeddingService         (ONNX inference)
  └── EmbeddingWorkerOptions   (BatchSize, IntervalSeconds)

EmbeddingQueue
  └── EngineRegistry           (accesso al system engine per _emb_queue)
```
