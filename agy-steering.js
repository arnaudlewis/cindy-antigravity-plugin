'use strict';

const { Transform } = require('node:stream');
const { StringDecoder } = require('node:string_decoder');
const { completeInitializeResult, versionSupported } = require('./remote-workspace.js');

function lineTransform(handle) {
  const decoder = new StringDecoder('utf8');
  let pending = '';
  function drain(stream) {
    let start = 0;
    let end;
    while ((end = pending.indexOf('\n', start)) !== -1) {
      const line = pending.slice(start, end + 1);
      const forwarded = handle(line);
      if (forwarded !== null) stream.push(forwarded);
      start = end + 1;
    }
    pending = pending.slice(start);
    if (Buffer.byteLength(pending) > 16 * 1024 * 1024) throw new Error('ACP line exceeds 16 MiB');
  }
  return new Transform({
    transform(chunk, _encoding, callback) {
      try {
        pending += decoder.write(chunk);
        drain(this);
        callback();
      } catch (error) {
        callback(error);
      }
    },
    flush(callback) {
      pending += decoder.end();
      callback(null, pending);
    },
  });
}

const encode = (message) => JSON.stringify(message) + '\n';
function parse(line) {
  try { return JSON.parse(line); } catch { return null; }
}

function supportsSteering(info) {
  if (info?.name !== 'Antigravity ACP Server') return false;
  const version = /^1\.(\d+)\.\d+$/.exec(info.version);
  return !!version && Number(version[1]) >= 5;
}

/**
 * agy 1.5.13+ connects to HTTP MCP servers passed in session/new without declaring
 * `agentCapabilities.mcpCapabilities.http`, so Cindy leaves them out. Declare it when it
 * is omitted; an explicit boolean is the agent's own answer and stays.
 */
function completeHttpMcp(message) {
  const result = message.result;
  if (!versionSupported(result?.agentInfo?.version)) return message;
  if (typeof result.agentCapabilities?.mcpCapabilities?.http === 'boolean') return message;
  return { ...message, result: completeInitializeResult(result) };
}

/**
 * agy accepts concurrent session/prompt requests. The previous request may end
 * as soon as a new one takes over. Keep Cindy's original request open until
 * the newest prompt finishes and relay all of the session's streamed updates.
 */
function attachSteering(input, output, child, stop) {
  const turns = new Map();
  const requests = new Map();
  let nextSteerId = 0;
  let initializeId;
  let offered = false;

  const toClient = lineTransform((line) => {
    const message = parse(line);
    if (!message || message.method || message.id === undefined) return line;
    if (message.id === initializeId) {
      initializeId = undefined;
      const completed = completeHttpMcp(message);
      if (supportsSteering(completed.result?.agentInfo)) {
        offered = true;
        const result = completed.result;
        return encode({ ...completed, result: { ...result, _meta: {
          ...result._meta, steering: { ...result._meta?.steering, supported: true },
        } } });
      }
      if (completed !== message) return encode(completed);
    }

    const turn = requests.get(message.id);
    if (!turn) return line;
    requests.delete(message.id);
    if (message.id === turn.latestId) turn.result = message;
    if (--turn.pending !== 0) return null;
    turns.delete(turn.sessionId);
    return encode({ ...turn.result, id: turn.originalId });
  });

  const toAgent = lineTransform((line) => {
    const message = parse(line);
    if (!message?.method || message.id === undefined) return line;
    const params = message.params;
    if (message.method === 'initialize') initializeId = message.id;
    if (!offered) return line;
    if (message.method === 'session/prompt' && typeof params?.sessionId === 'string' && !turns.has(params.sessionId)) {
      const turn = { sessionId: params.sessionId, originalId: message.id, latestId: message.id, pending: 1 };
      turns.set(params.sessionId, turn);
      requests.set(message.id, turn);
    }
    if (message.method !== '_session/steering') return line;
    const turn = turns.get(params?.sessionId);
    if (!turn || turn.result) {
      toClient.push(encode({ jsonrpc: '2.0', id: message.id, result: {
        outcome: 'promptRequired', reason: 'noRunningTurn',
      } }));
      return null;
    }
    const id = 'cindy-steer-' + ++nextSteerId;
    turn.latestId = id;
    turn.pending++;
    requests.set(id, turn);
    // Acknowledge the bridge accepting input before agy can emit its output.
    // Native submission failures then surface on the original prompt.
    toClient.push(encode({ jsonrpc: '2.0', id: message.id, result: { outcome: 'injected' } }));
    return encode({ jsonrpc: '2.0', id, method: 'session/prompt', params: {
      sessionId: turn.sessionId, prompt: params.prompt,
    } });
  });
  toAgent.on('error', stop);
  toClient.on('error', stop);
  input.pipe(toAgent).pipe(child.stdin);
  child.stdout.pipe(toClient).pipe(output, { end: false });
  return () => input.unpipe(toAgent);
}

module.exports = { attachSteering };
