import { expect, test } from 'bun:test';

import { openAIResponsesAdapter } from '@aio-proxy/core';
import { ProviderProtocol } from '@aio-proxy/types';

import {
  defineProviderRouteSource,
  REQUESTED_MODEL,
  rawProvider,
  retryConfig,
  settleRecording,
  withSnapshotConfigs,
} from '../../../__tests__/pipeline-helpers';
import { LogicalSessionStore } from '../../logical-session-store';
import type { ProviderRouteSource } from '../../runtime';
import { createUsageCapture } from '../../usage-capture';
import { handleProtocolRequest } from './index';

// ChatGPT's OAuth Responses path returns HTTP 200, then `response.failed` with
// `server_is_overloaded` / `service_unavailable_error` and a multi-minute
// retry_after. The fixture is the normalized SSE shape createOpenAIStreamFetch
// emits after `response.created`.
const OVERLOAD_RETRY_AFTER_SECONDS = 308;

function overloadStream(responseId: string): string {
  return (
    'event: response.created\n' +
    `data: {"type":"response.created","response":{"id":"${responseId}","status":"in_progress"}}\n\n` +
    'event: response.failed\n' +
    'data: ' +
    JSON.stringify({
      type: 'response.failed',
      response: {
        id: responseId,
        status: 'failed',
        error: {
          type: 'service_unavailable_error',
          code: 'server_is_overloaded',
          message: 'Selected model is at capacity. Please try a different model.',
          retry_after: OVERLOAD_RETRY_AFTER_SECONDS,
        },
      },
    }) +
    '\n\n'
  );
}

const SUCCESS_STREAM =
  'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_backup","status":"in_progress"}}\n\n' +
  'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"OK"}\n\n' +
  'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_backup","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"OK"}]}]}}\n\n';

const OUTPUT_THEN_OVERLOAD =
  'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"partial"}\n\n' +
  overloadStream('resp_after_output');

function sse(body: string): Response {
  return new Response(body, { status: 200, headers: { 'content-type': 'text/event-stream' } });
}

function responsesRequest(): Request {
  return new Request('https://proxy.test/v1/responses', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      model: REQUESTED_MODEL,
      input: 'ping',
      stream: true,
      prompt_cache_key: 'codex-session',
    }),
  });
}

function routed(providers: Parameters<typeof defineProviderRouteSource>[0], failoverOnStreamOverload = true) {
  const route = defineProviderRouteSource(providers);
  const source: ProviderRouteSource = {
    ...withSnapshotConfigs(route.source, retryConfig({ failoverOnStreamOverload })),
    logicalSessionStore: new LogicalSessionStore({
      repository: {
        resolveResponse: () => undefined,
        findAffinity: () => ({ providerId: 'chatgpt-oauth', revision: 1, active: true }),
      },
    }),
    usageCapture: createUsageCapture(),
  };
  return { route, source };
}

test('fails over immediately when the affinity provider streams capacity overload', async () => {
  const started = performance.now();
  const primary = rawProvider({
    id: 'chatgpt-oauth',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 0,
    invoke: async () => sse(overloadStream('resp_overloaded')),
  });
  const backup = rawProvider({
    id: 'carpool-responses',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 10,
    invoke: async () => sse(SUCCESS_STREAM),
  });
  const { route, source } = routed([primary, backup]);

  const response = await handleProtocolRequest({
    adapter: openAIResponsesAdapter,
    context: {},
    rawRequest: responsesRequest(),
    source,
  });
  const body = await response.text();
  await settleRecording(route.recording);

  expect(response.status).toBe(200);
  expect(body).toContain('resp_backup');
  expect(body).toContain('OK');
  expect(body).not.toContain('server_is_overloaded');
  expect(primary.calls.raw).toHaveLength(1);
  expect(backup.calls.raw).toHaveLength(1);
  expect(performance.now() - started).toBeLessThan(OVERLOAD_RETRY_AFTER_SECONDS * 1_000);
  expect(route.recording.attempts).toEqual([
    expect.objectContaining({
      providerId: 'chatgpt-oauth',
      outcome: 'failure',
      selectionReason: 'affinity',
      errorCode: 'stream_overload',
    }),
    expect.objectContaining({
      providerId: 'carpool-responses',
      outcome: 'success',
      selectionReason: 'weight',
    }),
  ]);
  expect(route.recording.attempts[1]?.errorCode).toBeUndefined();
  expect(route.recording.finals[0]).toEqual(
    expect.objectContaining({ outcome: 'success', finalProviderId: 'carpool-responses' }),
  );
  const missed = source.logicalSessionStore.begin({
    headers: new Headers(),
    hints: { candidates: [], previousResponseId: 'resp_overloaded', transcript: 'next' },
  });
  expect(missed.responseStatus).toBe('none');
  const owned = source.logicalSessionStore.begin({
    headers: new Headers(),
    hints: { candidates: [], previousResponseId: 'resp_backup', transcript: 'next' },
  });
  expect(owned.responseStatus).toBe('owned');
  if (owned.responseStatus === 'owned') expect(owned.responseOwner.providerId).toBe('carpool-responses');
});

test('does not fail over a completed 200 Responses stream', async () => {
  const primary = rawProvider({
    id: 'chatgpt-oauth',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 0,
    invoke: async () => sse(SUCCESS_STREAM),
  });
  const backup = rawProvider({
    id: 'carpool-responses',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 10,
    invoke: async () => sse(SUCCESS_STREAM),
  });
  const { route, source } = routed([primary, backup]);

  const response = await handleProtocolRequest({
    adapter: openAIResponsesAdapter,
    context: {},
    rawRequest: responsesRequest(),
    source,
  });
  const body = await response.text();
  await settleRecording(route.recording);

  expect(response.status).toBe(200);
  expect(body).toContain('response.completed');
  expect(primary.calls.raw).toHaveLength(1);
  expect(backup.calls.raw).toHaveLength(0);
  expect(route.recording.attempts).toEqual([
    expect.objectContaining({ providerId: 'chatgpt-oauth', outcome: 'success', selectionReason: 'affinity' }),
  ]);
});

const OTHER_FAILURE =
  'event: response.failed\ndata: {"type":"response.failed","response":{"id":"resp_other","status":"failed","error":{"type":"invalid_request_error","code":"context_too_large","message":"too big","retry_after":308}}}\n\n';

test('does not fail over a 200 stream whose failure is not capacity overload', async () => {
  const primary = rawProvider({
    id: 'chatgpt-oauth',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 0,
    invoke: async () => sse(OTHER_FAILURE),
  });
  const backup = rawProvider({
    id: 'carpool-responses',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 10,
    invoke: async () => sse(SUCCESS_STREAM),
  });
  const { route, source } = routed([primary, backup]);

  const response = await handleProtocolRequest({
    adapter: openAIResponsesAdapter,
    context: {},
    rawRequest: responsesRequest(),
    source,
  });
  const body = await response.text();
  await settleRecording(route.recording);

  expect(body).toContain('context_too_large');
  expect(primary.calls.raw).toHaveLength(1);
  expect(backup.calls.raw).toHaveLength(0);
  expect(route.recording.finals[0]).toEqual(
    expect.objectContaining({ outcome: 'failure', finalProviderId: 'chatgpt-oauth', finalStatusCode: 200 }),
  );
});

test('does not fail over an overload that arrives after generated output', async () => {
  const primary = rawProvider({
    id: 'chatgpt-oauth',
    protocol: ProviderProtocol.OpenAIResponse,
    invoke: async () => sse(OUTPUT_THEN_OVERLOAD),
  });
  const backup = rawProvider({
    id: 'carpool-responses',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 10,
    invoke: async () => sse(SUCCESS_STREAM),
  });
  const { route, source } = routed([primary, backup]);

  const response = await handleProtocolRequest({
    adapter: openAIResponsesAdapter,
    context: {},
    rawRequest: responsesRequest(),
    source,
  });
  const body = await response.text();
  await settleRecording(route.recording);

  expect(body).toContain('partial');
  expect(body).toContain('server_is_overloaded');
  expect(primary.calls.raw).toHaveLength(1);
  expect(backup.calls.raw).toHaveLength(0);
  expect(route.recording.finals[0]).toEqual(
    expect.objectContaining({ outcome: 'failure', finalProviderId: 'chatgpt-oauth', finalStatusCode: 200 }),
  );
});

test('surfaces the overload stream when every candidate is overloaded', async () => {
  const started = performance.now();
  const primary = rawProvider({
    id: 'chatgpt-oauth',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 0,
    invoke: async () => sse(overloadStream('resp_a')),
  });
  const backup = rawProvider({
    id: 'carpool-responses',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 10,
    invoke: async () => sse(overloadStream('resp_b')),
  });
  const { route, source } = routed([primary, backup]);

  const response = await handleProtocolRequest({
    adapter: openAIResponsesAdapter,
    context: {},
    rawRequest: responsesRequest(),
    source,
  });
  const body = await response.text();
  await settleRecording(route.recording);

  expect(response.status).toBe(200);
  expect(body).toContain('resp_b');
  expect(body).toContain('server_is_overloaded');
  expect(body).not.toContain('resp_a');
  expect(primary.calls.raw).toHaveLength(1);
  expect(backup.calls.raw).toHaveLength(1);
  expect(performance.now() - started).toBeLessThan(OVERLOAD_RETRY_AFTER_SECONDS * 1_000);
  expect(route.recording.attempts).toEqual([
    expect.objectContaining({ providerId: 'chatgpt-oauth', outcome: 'failure', errorCode: 'stream_overload' }),
    expect.objectContaining({ providerId: 'carpool-responses', outcome: 'failure' }),
  ]);
  expect(route.recording.attempts[1]?.errorCode).toBeUndefined();
  expect(route.recording.finals[0]).toEqual(
    expect.objectContaining({ outcome: 'failure', finalProviderId: 'carpool-responses', finalStatusCode: 200 }),
  );
});

test('keeps the overload stream on the affinity provider when failover is disabled', async () => {
  const primary = rawProvider({
    id: 'chatgpt-oauth',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 0,
    invoke: async () => sse(overloadStream('resp_overloaded')),
  });
  const backup = rawProvider({
    id: 'carpool-responses',
    protocol: ProviderProtocol.OpenAIResponse,
    priority: 10,
    invoke: async () => sse(SUCCESS_STREAM),
  });
  const { route, source } = routed([primary, backup], false);

  const response = await handleProtocolRequest({
    adapter: openAIResponsesAdapter,
    context: {},
    rawRequest: responsesRequest(),
    source,
  });
  const body = await response.text();
  await settleRecording(route.recording);

  expect(body).toContain('server_is_overloaded');
  expect(primary.calls.raw).toHaveLength(1);
  expect(backup.calls.raw).toHaveLength(0);
  expect(route.recording.finals[0]).toEqual(
    expect.objectContaining({ outcome: 'failure', finalProviderId: 'chatgpt-oauth', finalStatusCode: 200 }),
  );
});
