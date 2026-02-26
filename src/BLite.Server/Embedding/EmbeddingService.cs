// BLite.Server — EmbeddingService: ONNX semantic embedding singleton
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Wraps an ONNX sentence-transformers model + HuggingFace tokenizer.
// Hot-swap is protected by a ReaderWriterLockSlim so Embed() calls in
// progress complete safely while the model directory is being replaced.

using System.Text.Json;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;
using Microsoft.ML.Tokenizers;

namespace BLite.Server.Embedding;

public sealed record EmbeddingModelInfo(
    string Directory,
    string ModelName,
    int Dimension);

public sealed class EmbeddingService : IDisposable
{
    private readonly ReaderWriterLockSlim _rwLock = new();
    private readonly int _maxTokens;
    private readonly ILogger<EmbeddingService> _logger;

    private InferenceSession? _session;
    private BertTokenizer? _tokenizer;
    private EmbeddingModelInfo _info;

    /// <summary>True when a model has been successfully loaded and <see cref="Embed"/> can be called.</summary>
    public bool IsLoaded
    {
        get
        {
            _rwLock.EnterReadLock();
            try { return _session != null; }
            finally { _rwLock.ExitReadLock(); }
        }
    }

    public EmbeddingService(IConfiguration config, ILogger<EmbeddingService> logger)
    {
        _logger = logger;
        var dir = config.GetValue<string>("Embedding:ModelDirectory") ?? "Embedding/MiniLM-L6-v2";
        _maxTokens = config.GetValue<int?>("Embedding:MaxTokens") ?? 512;
        _info = new EmbeddingModelInfo(dir, Path.GetFileName(dir), 0);

        // Model loading is optional at startup — the service starts in 'no model' mode
        // if the directory or files are absent.  Embed() will throw until a model is loaded.
        if (Directory.Exists(dir))
        {
            try
            {
                Load(dir);
            }
            catch (FileNotFoundException ex)
            {
                _logger.LogWarning("Embedding model not loaded: {Message}. " +
                    "Configure Embedding:ModelDirectory or call LoadFromDirectory() at runtime.",
                    ex.Message);
            }
        }
        else
        {
            _logger.LogInformation(
                "Embedding model directory '{Dir}' not found — embedding disabled until a model is loaded.",
                dir);
        }
    }

    public EmbeddingModelInfo Info
    {
        get
        {
            _rwLock.EnterReadLock();
            try { return _info; }
            finally { _rwLock.ExitReadLock(); }
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Validates <paramref name="directory"/>, builds a new model from it and
    /// atomically replaces the current one. The old session is disposed after swap.
    /// </summary>
    public void LoadFromDirectory(string directory)
    {
        var (session, tokenizer, dim) = Build(directory);

        _rwLock.EnterWriteLock();
        try
        {
            var prev = _session;
            _session = session;
            _tokenizer = tokenizer;
            _info = new EmbeddingModelInfo(directory, Path.GetFileName(directory), dim);
            prev?.Dispose();
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    /// <summary>Encodes <paramref name="text"/> and returns an L2-normalised float vector.</summary>
    public float[] Embed(string text)
    {
        _rwLock.EnterReadLock();
        try
        {
            if (_session is null || _tokenizer is null)
                throw new InvalidOperationException("Embedding model is not loaded.");

            return RunInference(_session, _tokenizer, text, _maxTokens);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private void Load(string directory)
    {
        var (session, tokenizer, dim) = Build(directory);
        _session = session;
        _tokenizer = tokenizer;
        _info = new EmbeddingModelInfo(directory, Path.GetFileName(directory), dim);
    }

    private static (InferenceSession Session, BertTokenizer Tokenizer, int Dimension) Build(string directory)
    {
        var modelPath = Path.Combine(directory, "model.onnx");
        var tokenizerPath = Path.Combine(directory, "tokenizer.json");
        var tokenizerConfigPath = Path.Combine(directory, "tokenizer_config.json");

        if (!File.Exists(modelPath))
            throw new FileNotFoundException("model.onnx not found in the specified directory.", modelPath);
        if (!File.Exists(tokenizerPath))
            throw new FileNotFoundException("tokenizer.json not found in the specified directory.", tokenizerPath);

        var opts = new Microsoft.ML.OnnxRuntime.SessionOptions
        {
            GraphOptimizationLevel = GraphOptimizationLevel.ORT_ENABLE_ALL
        };
        var session = new InferenceSession(modelPath, opts);

        // Last dimension of the first output tensor is the embedding size.
        var dims = session.OutputMetadata.Values.First().Dimensions;
        int dim = dims.Length > 0 ? dims[^1] : 384;

        // Read do_lower_case from tokenizer_config.json — defaults to true (uncased models).
        bool doLowerCase = true;
        if (File.Exists(tokenizerConfigPath))
        {
            using var cfgDoc = JsonDocument.Parse(File.ReadAllBytes(tokenizerConfigPath));
            if (cfgDoc.RootElement.TryGetProperty("do_lower_case", out var lc))
                doLowerCase = lc.GetBoolean();
        }

        // BertTokenizer.Create expects a vocab.txt stream (one token per line, index = id).
        // The vocab is embedded in tokenizer.json under model.vocab as a {token: id} map.
        using var vocabStream = ExtractVocabStream(tokenizerPath);
        var bertOpts = new BertOptions { LowerCaseBeforeTokenization = doLowerCase };
        var tokenizer = BertTokenizer.Create(vocabStream, bertOpts);

        return (session, tokenizer, dim);
    }

    /// <summary>
    /// Parses the HuggingFace tokenizer.json, extracts model.vocab and writes
    /// a vocab.txt-style stream (token per line sorted by token ID).
    /// </summary>
    private static MemoryStream ExtractVocabStream(string tokenizerPath)
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(tokenizerPath));
        var vocab = doc.RootElement.GetProperty("model").GetProperty("vocab");

        var sorted = new SortedDictionary<int, string>();
        foreach (var prop in vocab.EnumerateObject())
            sorted[prop.Value.GetInt32()] = prop.Name;

        var ms = new MemoryStream();
        var sw = new StreamWriter(ms, leaveOpen: true);
        foreach (var token in sorted.Values)
            sw.WriteLine(token);
        sw.Flush();
        ms.Position = 0;
        return ms;
    }

    private static float[] RunInference(
        InferenceSession session,
        BertTokenizer tokenizer,
        string text,
        int maxTokens)
    {
        // addSpecialTokens=true adds CLS/SEP; considerPreTokenization=true applies whitespace split
        var ids = tokenizer.EncodeToIds(text, addSpecialTokens: true, considerPreTokenization: true);

        int seqLen = Math.Min(ids.Count, maxTokens);
        long[] iArr = ids.Take(seqLen).Select(id => (long)id).ToArray();
        long[] mask = new long[seqLen];
        long[] typeIds = new long[seqLen];

        Array.Fill(mask, 1L);

        int[] shape = [1, seqLen];

        var inputIds = new DenseTensor<long>(iArr, shape);
        var attentionMask = new DenseTensor<long>(mask, shape);
        var tokenTypeIds = new DenseTensor<long>(typeIds, shape);

        var inputs = new List<NamedOnnxValue>
        {
            NamedOnnxValue.CreateFromTensor("input_ids", inputIds),
            NamedOnnxValue.CreateFromTensor("attention_mask", attentionMask),
        };

        // token_type_ids is optional depending on how the model was exported
        if (session.InputMetadata.ContainsKey("token_type_ids"))
            inputs.Add(NamedOnnxValue.CreateFromTensor("token_type_ids", tokenTypeIds));

        using var results = session.Run(inputs);
        var output = results.First().AsTensor<float>();

        int hidden = output.Dimensions[2];
        var pooled = new float[hidden];

        // Mean-pool over the sequence dimension (all tokens are non-padding here)
        for (int t = 0; t < seqLen; t++)
            for (int h = 0; h < hidden; h++)
                pooled[h] += output[0, t, h];

        for (int h = 0; h < hidden; h++)
            pooled[h] /= seqLen;

        // L2 normalise
        float norm = 0f;
        for (int h = 0; h < hidden; h++)
            norm += pooled[h] * pooled[h];
        norm = MathF.Sqrt(norm);

        if (norm > 0f)
            for (int h = 0; h < hidden; h++)
                pooled[h] /= norm;

        return pooled;
    }

    public void Dispose()
    {
        _session?.Dispose();
        _rwLock.Dispose();
    }
}

