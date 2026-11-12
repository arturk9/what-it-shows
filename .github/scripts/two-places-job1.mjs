import { createCipheriv, randomBytes, publicEncrypt, constants } from 'node:crypto';
import { readFile } from 'node:fs/promises';

const endpoint = process.env.MEGA_JOB_1_MCP_URL;
if (!endpoint) throw new Error('Missing MEGA_JOB_1_MCP_URL');
const publicKey = await readFile(new URL('./job1-result-public.pem', import.meta.url), 'utf8');
const trace = [];
let sessionId;
let protocolVersion = '2025-03-26';
let nextId = 1;
const secretValues = new Set();

function parsePayload(raw) {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  if (!trimmed.startsWith('event:') && !trimmed.includes('\ndata:')) return JSON.parse(trimmed);
  const messages = [];
  for (const block of trimmed.split(/\n\n+/)) {
    const data = block.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim()).join('\n');
    if (data && data !== '[DONE]') {
      try { messages.push(JSON.parse(data)); } catch {}
    }
  }
  if (!messages.length) throw new Error('MCP returned an unreadable event stream');
  return messages.at(-1);
}

function mask(value, key = '') {
  if (/token|receipt/i.test(key)) return '[REDACTED]';
  if (Array.isArray(value)) return value.map((item) => mask(item));
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, mask(v, k)]));
  }
  if (typeof value === 'string') {
    let output = value
      .replace(/https:\/\/hq\.mega\.dev\/mcp\/job\/[^\s"']+/g, '[PRIVATE MCP URL]')
      .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, '[REDACTED TOKEN]');
    for (const secret of secretValues) if (secret) output = output.replaceAll(secret, '[REDACTED TOKEN]');
    return output;
  }
  return value;
}

async function post(message, { capture = true } = {}) {
  const headers = {
    accept: 'application/json, text/event-stream',
    'content-type': 'application/json',
  };
  if (sessionId) headers['mcp-session-id'] = sessionId;
  if (protocolVersion) headers['mcp-protocol-version'] = protocolVersion;
  const response = await fetch(endpoint, { method: 'POST', headers, body: JSON.stringify(message) });
  sessionId = response.headers.get('mcp-session-id') || sessionId;
  const raw = await response.text();
  let parsed = null;
  if (raw.trim()) parsed = parsePayload(raw);
  if (parsed) {
    const returnedToken = findToken(parsed);
    const returnedReceipt = findNamedDeep(parsed, /^receipt$/i);
    if (returnedToken) secretValues.add(String(returnedToken));
    if (returnedReceipt) secretValues.add(String(returnedReceipt));
  }
  if (capture) trace.push({ request: mask(message), reply: { status: response.status, body: mask(parsed ?? raw) } });
  if (!response.ok) throw new Error(`MCP HTTP ${response.status}`);
  if (parsed?.error) throw new Error(`MCP error ${parsed.error.code}: ${parsed.error.message}`);
  return parsed;
}

async function rpc(method, params = {}) {
  const id = nextId++;
  const response = await post({ jsonrpc: '2.0', id, method, params });
  if (!response || response.id !== id || !response.result) throw new Error(`Invalid MCP response for ${method}`);
  return response.result;
}

function walk(value, visit, path = []) {
  if (!value || typeof value !== 'object') return;
  visit(value, path);
  if (Array.isArray(value)) value.forEach((v, i) => walk(v, visit, [...path, String(i)]));
  else for (const [k, v] of Object.entries(value)) walk(v, visit, [...path, k]);
}

function findToken(value) {
  let found;
  walk(value, (node) => {
    if (found || Array.isArray(node)) return;
    for (const [key, val] of Object.entries(node)) {
      if (typeof val === 'string' && /^(step_?token|next_?step_?token|step|token)$/i.test(key)) found = val;
    }
  });
  return found;
}

function normalizeToolResult(result) {
  if (result?.isError) throw new Error('Job tool returned an error');
  if (result?.structuredContent !== undefined) return result.structuredContent;
  const texts = (result?.content ?? []).filter((item) => item.type === 'text').map((item) => item.text);
  for (const text of texts) {
    try { return JSON.parse(text); } catch {}
  }
  if (texts.length === 1) return texts[0];
  return result;
}

let tools;
let stepToken;
async function callTool(name, args) {
  const definition = tools.find((tool) => tool.name === name);
  if (!definition) throw new Error(`Required job tool missing: ${name}`);
  const properties = definition.inputSchema?.properties ?? {};
  const tokenKey = Object.keys(properties).find((key) => /^(step_?token|next_?step_?token|step|token)$/i.test(key));
  const input = { ...args };
  if (tokenKey && stepToken) {
    secretValues.add(stepToken);
    input[tokenKey] = stepToken;
  }
  const result = await rpc('tools/call', { name, arguments: input });
  const output = normalizeToolResult(result);
  const returnedToken = findToken(output);
  if (returnedToken) stepToken = returnedToken;
  return output;
}

function toPlainObject(value) {
  if (typeof value === 'string') {
    try { return JSON.parse(value); } catch { return { text: value }; }
  }
  return value;
}

function findTramRecord(value) {
  let candidates = [];
  walk(value, (node) => {
    if (Array.isArray(node)) return;
    const serialized = JSON.stringify(node);
    if (/tram[- ]?3/i.test(serialized)) candidates.push(node);
  });
  candidates = candidates.filter((node) => Object.keys(node).length > 1);
  candidates.sort((a, b) => {
    const hasLineA = Object.entries(a).some(([key, value]) => /line|tram|vehicle/i.test(key) && typeof value === 'string' && /tram[- ]?3/i.test(value));
    const hasLineB = Object.entries(b).some(([key, value]) => /line|tram|vehicle/i.test(key) && typeof value === 'string' && /tram[- ]?3/i.test(value));
    if (hasLineA !== hasLineB) return hasLineA ? -1 : 1;
    return Object.keys(a).length - Object.keys(b).length;
  });
  return candidates[0];
}

function findValueByPattern(value, pattern) {
  return findNamedDeep(value, pattern);
}

function findNamedDeep(value, pattern) {
  let found;
  walk(value, (node) => {
    if (found || Array.isArray(node)) return;
    for (const [key, val] of Object.entries(node)) {
      if (pattern.test(key) && (typeof val === 'string' || typeof val === 'number')) {
        found = val;
        return;
      }
    }
  });
  return found;
}

function firstProperty(properties, pattern, fallback) {
  return Object.keys(properties).find((key) => pattern.test(key)) ?? fallback;
}

function readRecord(value) {
  let record;
  walk(value, (node) => {
    if (Array.isArray(node)) return;
    const keys = Object.keys(node).map((key) => key.toLowerCase());
    if (['date', 'time', 'clock', 'line', 'cause'].every((key) => keys.includes(key))) record = node;
  });
  return record;
}

function field(record, name) {
  const key = Object.keys(record ?? {}).find((item) => item.toLowerCase() === name);
  if (!key || record[key] === undefined || record[key] === null) throw new Error(`Missing requested field: ${name}`);
  return String(record[key]);
}

function exactText(value) {
  if (typeof value === 'string') return value;
  for (const key of ['text', 'redacted', 'body', 'record', 'value']) {
    if (typeof value?.[key] === 'string') return value[key];
  }
  throw new Error('Could not find exact text returned by redact');
}

async function run() {
  const init = await rpc('initialize', {
    protocolVersion,
    capabilities: {},
    clientInfo: { name: 'two-places-job1-ci-worker', version: '1.0.0' },
  });
  protocolVersion = init.protocolVersion || protocolVersion;
  await post({ jsonrpc: '2.0', method: 'notifications/initialized' });
  const listing = await rpc('tools/list');
  tools = listing.tools ?? [];

  const search = await callTool('search', { query: 'tram-3', scope: 'ops/live' });
  const match = findTramRecord(toPlainObject(search));
  if (!match) throw new Error('No tram-3 incident in ops/live search results');
  const readTool = tools.find((tool) => tool.name === 'read');
  const readProps = readTool?.inputSchema?.properties ?? {};
  const idKey = firstProperty(readProps, /(^id$|record|entry|incident)/i, 'id');
  const recordId = findValueByPattern(match, /(^id$|record|entry|incident)/i);
  if (recordId === undefined) throw new Error('Could not identify the tram-3 record ID');
  const fieldsKey = firstProperty(readProps, /^fields$/i, 'fields');
  const read = await callTool('read', { [idKey]: recordId, [fieldsKey]: ['date', 'time', 'clock', 'line', 'cause'] });
  const record = readRecord(toPlainObject(read));
  if (!record) throw new Error('Read reply did not contain all requested fields');
  const original = ['date', 'time', 'clock', 'line', 'cause'].map((name) => `${name}: ${field(record, name)}`).join('\n');

  const redactTool = tools.find((tool) => tool.name === 'redact');
  const redactProps = redactTool?.inputSchema?.properties ?? {};
  const textKey = firstProperty(redactProps, /text|body|record/i, 'text');
  const redacted = await callTool('redact', { [textKey]: original });
  const finalText = exactText(toPlainObject(redacted));

  const submitTool = tools.find((tool) => tool.name === 'submit');
  const submitProps = submitTool?.inputSchema?.properties ?? {};
  const submitTextKey = firstProperty(submitProps, /^(result|text|body|record)$/i, 'result');
  const versionKey = firstProperty(submitProps, /skill.?version/i, 'skillVersion');
  const submitted = await callTool('submit', { [submitTextKey]: finalText, [versionKey]: '1' });
  const outcome = toPlainObject(submitted);
  const receipt = findValueByPattern(outcome, /^receipt$/i);
  if (receipt === undefined) throw new Error('Submit reply did not contain a receipt');
  const accepted = outcome?.result ?? outcome?.acceptedResult ?? outcome?.answer ?? finalText;

  const payload = JSON.stringify({ status: 'accepted', result: accepted, receipt, trace });
  const key = randomBytes(32);
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(payload, 'utf8'), cipher.final()]);
  const wrappedKey = publicEncrypt({ key: publicKey, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' }, key);
  const envelope = {
    version: 1,
    key: wrappedKey.toString('base64'),
    iv: iv.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
    data: ciphertext.toString('base64'),
  };
  process.stdout.write(`SEALED_RESULT:${Buffer.from(JSON.stringify(envelope)).toString('base64')}\n`);
}

run().catch((error) => {
  const safe = String(error?.message ?? 'worker failed')
    .replaceAll(endpoint, '[PRIVATE MCP URL]')
    .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, '[REDACTED TOKEN]');
  const payload = JSON.stringify({ status: 'failed', error: safe, trace });
  const key = randomBytes(32);
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(payload, 'utf8'), cipher.final()]);
  const wrappedKey = publicEncrypt({ key: publicKey, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' }, key);
  const envelope = { version: 1, key: wrappedKey.toString('base64'), iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), data: ciphertext.toString('base64') };
  process.stdout.write(`SEALED_DIAGNOSTIC:${Buffer.from(JSON.stringify(envelope)).toString('base64')}\n`);
  process.stderr.write('job-1 worker failed; encrypted diagnostics were emitted.\n');
  process.exitCode = 1;
});
