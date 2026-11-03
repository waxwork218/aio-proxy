import { isPlainObject } from 'es-toolkit/predicate';

// Shared parsing of the error payloads the Responses API and the ChatGPT
// backend return. Private to `protocol/openai-responses/`: both raw-retry hooks
// classify the same envelopes, and a divergent walk between them would let one
// hook read a code the other missed.

export function parseErrorPayload(text: string): Record<string, unknown> | undefined {
  try {
    const value: unknown = JSON.parse(text);
    return isPlainObject(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

// Official Responses `event: error` puts `code` on the payload root. ChatGPT
// raw traffic then goes through createOpenAIStreamFetch, which rewrites that
// frame to `response.failed` and stores the original object at
// `response.error` — sometimes wrapping a nested `{ error: { code } }`.
export function responsesErrorCode(payload: Record<string, unknown> | undefined): string | undefined {
  if (payload === undefined) return undefined;
  return codeFrom(errorChain(payload)) ?? codeFrom(errorChain(responseEnvelope(payload)));
}

export function responsesErrorMessage(payload: Record<string, unknown> | undefined): string | undefined {
  if (payload === undefined) return undefined;
  return messageFrom(errorChain(payload)) ?? messageFrom(errorChain(responseEnvelope(payload)));
}

function responseEnvelope(payload: Record<string, unknown>): Record<string, unknown> | undefined {
  return isPlainObject(payload['response']) ? payload['response'] : undefined;
}

// Both lookups walk this one chain, so a code can never sit on a node whose
// message was consulted: a depth mismatch would let an explicit non-matching
// code look absent and hand a rewrite-and-resend to the prose fallback.
// Iterative and depth-capped because the chain is provider-controlled — a
// recursive walk over a deeply nested `error` chain (~50k levels fit under the
// 1 MiB body cap) would throw a RangeError out of `classify` instead of
// committing and forwarding the provider's response.
const MAX_ERROR_NESTING = 8;

function errorChain(root: Record<string, unknown> | undefined): readonly Record<string, unknown>[] {
  if (root === undefined) return [];
  const chain: Record<string, unknown>[] = [root];
  for (let node = root['error']; isPlainObject(node) && chain.length < MAX_ERROR_NESTING; node = node['error']) {
    chain.push(node);
  }
  return chain;
}

// Outermost wins: the envelope names the failure the provider is reporting.
function codeFrom(chain: readonly Record<string, unknown>[]): string | undefined {
  for (const node of chain) {
    const code = stringField(node, 'code');
    if (code !== undefined) return code;
  }
  return undefined;
}

// Innermost wins: a wrapper repeats or generalizes the message the backend
// actually issued.
function messageFrom(chain: readonly Record<string, unknown>[]): string | undefined {
  for (let index = chain.length - 1; index >= 0; index -= 1) {
    const message = stringField(chain[index]!, 'message');
    if (message !== undefined) return message;
  }
  return undefined;
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

// Event envelopes use these as their own `type`. They are not the provider
// error class, so a walk that stopped on them would hide `service_unavailable_error`
// nested under `response.error`.
const ENVELOPE_TYPES = new Set(['error', 'response.failed', 'response.incomplete', 'response.cancelled']);

// ChatGPT's Responses backend reports capacity as HTTP 200 plus one of these,
// often with a multi-minute `retry_after`. Either signal is enough: the code
// and the type do not always travel together.
const STREAM_OVERLOAD_CODES = new Set(['server_is_overloaded']);
const STREAM_OVERLOAD_TYPES = new Set(['service_unavailable_error']);

export function responsesErrorType(payload: Record<string, unknown> | undefined): string | undefined {
  if (payload === undefined) return undefined;
  return typeFrom(nestedErrors(payload)) ?? typeFrom(nestedErrors(responseEnvelope(payload)));
}

export function isResponsesStreamOverload(payload: Record<string, unknown> | undefined): boolean {
  if (payload === undefined) return false;
  const code = responsesErrorCode(payload);
  if (code !== undefined && STREAM_OVERLOAD_CODES.has(code)) return true;
  const type = responsesErrorType(payload);
  return type !== undefined && STREAM_OVERLOAD_TYPES.has(type);
}

function nestedErrors(root: Record<string, unknown> | undefined): readonly Record<string, unknown>[] {
  if (root === undefined) return [];
  const nested = root['error'];
  return isPlainObject(nested) ? errorChain(nested) : [];
}

function typeFrom(chain: readonly Record<string, unknown>[]): string | undefined {
  for (const node of chain) {
    const type = stringField(node, 'type');
    if (type !== undefined && !ENVELOPE_TYPES.has(type)) return type;
  }
  return undefined;
}
