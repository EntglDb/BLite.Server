// blite-client — gRPC stub loader
//
// Loads blite.proto at runtime using @grpc/proto-loader and creates typed
// gRPC stub instances pre-configured with the API-key channel credentials.

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

// Path to the bundled proto file (resolved at runtime relative to this file)
const PROTO_PATH = path.join(__dirname, '../proto/blite.proto');

const LOAD_OPTS: protoLoader.Options = {
  keepCase: true,
  longs: String,
  enums: Number,
  defaults: true,
  oneofs: true,
};

// ─── Types returned by proto-loader ──────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GrpcStubCtor = new (address: string, creds: grpc.ChannelCredentials, opts?: object) => any;

export interface BliteStubs {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dynamic:     any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  document:    any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  admin:       any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  transaction: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  metadata:    any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  kv:          any;
}

// ─── Exported loader ─────────────────────────────────────────────────────────

export function makeStubs(
  address: string,
  apiKey: string,
  useTls: boolean,
  opts?: object,
): BliteStubs {
  const pkgDef = protoLoader.loadSync(PROTO_PATH, LOAD_OPTS);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const proto = grpc.loadPackageDefinition(pkgDef) as any;
  const svc = proto.blite.v1;

  const creds = makeChannelCredentials(apiKey, useTls);

  // For insecure transport grpc-js forbids combining InsecureChannelCredentials
  // with CallCredentials, so we inject the API key via a channel interceptor instead.
  const channelOpts: object = useTls
    ? (opts ?? {})
    : { ...(opts ?? {}), interceptors: [makeApiKeyInterceptor(apiKey)] };

  const make = (Ctor: GrpcStubCtor) => new Ctor(address, creds, channelOpts);

  return {
    dynamic:     make(svc.DynamicService),
    document:    make(svc.DocumentService),
    admin:       make(svc.AdminService),
    transaction: make(svc.TransactionService),
    metadata:    make(svc.MetadataService),
    kv:          make(svc.KvService),
  };
}

/**
 * Returns gRPC channel credentials.
 * - TLS: SSL + per-call metadata generator (API key baked in).
 * - Insecure: plain insecure creds; API key is injected via an interceptor.
 */
export function makeChannelCredentials(
  apiKey: string,
  useTls: boolean,
): grpc.ChannelCredentials {
  if (useTls) {
    const callCreds = grpc.credentials.createFromMetadataGenerator((_args, cb) => {
      const meta = new grpc.Metadata();
      meta.add('x-api-key', apiKey);
      cb(null, meta);
    });
    return grpc.credentials.combineChannelCredentials(
      grpc.credentials.createSsl(),
      callCreds,
    );
  }
  return grpc.credentials.createInsecure();
}

/** Creates an interceptor that adds the x-api-key header to every outgoing call. */
function makeApiKeyInterceptor(apiKey: string): grpc.Interceptor {
  return (options, nextCall) => {
    return new grpc.InterceptingCall(nextCall(options), {
      start(metadata, listener, next) {
        metadata.add('x-api-key', apiKey);
        next(metadata, listener);
      },
    });
  };
}

// ─── Promisified gRPC call helpers ────────────────────────────────────────────

/** Wraps a unary gRPC stub call as a Promise. */
export function callUnary<Req, Res>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  stub: any,
  method: string,
  request: Req,
): Promise<Res> {
  return new Promise((resolve, reject) => {
    stub[method](request, (err: grpc.ServiceError | null, response: Res) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

/** Wraps a server-streaming gRPC call as an AsyncGenerator. */
export async function* callServerStream<Req, Res>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  stub: any,
  method: string,
  request: Req,
): AsyncGenerator<Res> {
  const call = stub[method](request) as grpc.ClientReadableStream<Res>;

  for await (const item of call) {
    yield item;
  }
}
