import http from 'http';
import path from 'path';
import express from 'express';
import { WebSocketServer } from 'ws';
import { v4 as uuid } from 'uuid';
import { fileURLToPath } from 'url';
import os from 'os';
import fs from 'fs';
import { promises as fsp } from 'fs';
import crypto from 'crypto';
import { Transform } from 'stream';
import { pipeline } from 'stream/promises';
import sanitizeFilename from 'sanitize-filename';
import { Bonjour } from 'bonjour-service';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 启动端口号
const DEFAULT_PORT = 3000;

const parsePort = (value) => {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) || parsed <= 0 ? null : parsed;
};

const extractCliPort = () => {
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === '--port' || arg === '-p') {
      return parsePort(args[index + 1]);
    }
    if (arg.startsWith('--port=')) {
      return parsePort(arg.split('=')[1]);
    }
  }
  return null;
};

const cliPort = extractCliPort();
const envPort = parsePort(process.env.PORT);
const PORT = cliPort ?? envPort ?? DEFAULT_PORT;
const HEARTBEAT_INTERVAL = 30000;
const WS_MAX_PAYLOAD_BYTES = 2 * 1024 * 1024;
const DEFAULT_HTTP_CHUNK_SIZE = 4 * 1024 * 1024;
const MAX_HTTP_CHUNK_SIZE = 16 * 1024 * 1024;
const TRANSFER_TTL = 2 * 60 * 60 * 1000;
const TRANSFER_CLEANUP_INTERVAL = 10 * 60 * 1000;
const TRANSFER_ROOT = process.env.SNAPSEND_TRANSFER_DIR
  ? path.resolve(process.env.SNAPSEND_TRANSFER_DIR)
  : path.join(os.tmpdir(), 'snapsend-transfers');
const SHA256_HEX = /^[a-f0-9]{64}$/i;
const ENABLE_MDNS = process.env.SNAPSEND_DISABLE_MDNS !== '1';

const ADJECTIVES = [
  '敏捷的',
  '迅捷的',
  '快乐的',
  '安静的',
  '温暖的',
  '璀璨的',
  '闪亮的',
  '勇敢的',
  '机智的',
  '悠然的',
  '灵动的',
  '轻盈的',
];

const NOUNS = [
  '西兰花',
  '星辰',
  '雨燕',
  '青竹',
  '微风',
  '山峦',
  '清泉',
  '花火',
  '晨露',
  '向日葵',
  '海豚',
  '薄荷',
];

const usedDisplayNames = new Set();

const generateUniqueDisplayName = () => {
  const totalCombinations = ADJECTIVES.length * NOUNS.length;
  for (let attempt = 0; attempt < totalCombinations; attempt += 1) {
    const adjective = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
    const noun = NOUNS[Math.floor(Math.random() * NOUNS.length)];
    const name = `${adjective}${noun}`;
    if (!usedDisplayNames.has(name)) {
      usedDisplayNames.add(name);
      return name;
    }
  }
  let fallback;
  do {
    fallback = `设备${uuid().slice(0, 4)}`;
  } while (usedDisplayNames.has(fallback));
  usedDisplayNames.add(fallback);
  return fallback;
};

const clients = new Map();
const transfers = new Map();
let bonjour = null;

const send = (ws, type, payload = {}) => {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({ type, payload }));
  }
};

const httpError = (statusCode, message) => {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
};

const asyncRoute = (handler) => (req, res, next) => {
  Promise.resolve(handler(req, res, next)).catch(next);
};

const randomToken = () => crypto.randomBytes(32).toString('hex');

const parseSafeInteger = (value) => {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
};

const normalizeFileName = (value) => {
  const cleaned =
    typeof value === 'string'
      ? value.trim().replace(/[\u0000-\u001f\u007f]+/g, '').slice(0, 255)
      : '';
  return cleaned || '未命名文件';
};

const normalizeMime = (value) => {
  if (typeof value !== 'string') return 'application/octet-stream';
  const trimmed = value.trim();
  if (!trimmed || trimmed.length > 255 || /[\r\n]/.test(trimmed)) {
    return 'application/octet-stream';
  }
  return trimmed;
};

const normalizeChunkSize = (value) => {
  const parsed = parseSafeInteger(value);
  if (!parsed) return DEFAULT_HTTP_CHUNK_SIZE;
  return Math.min(MAX_HTTP_CHUNK_SIZE, Math.max(64 * 1024, parsed));
};

const safeDownloadName = (name) => sanitizeFilename(name) || 'snapsend-file';

const getTransferToken = (req, headerName) => {
  const fromHeader = req.get(headerName);
  if (typeof fromHeader === 'string' && fromHeader) {
    return fromHeader;
  }
  const fromQuery = req.query.token;
  return typeof fromQuery === 'string' ? fromQuery : '';
};

const ensureTransferRoot = async () => {
  await fsp.mkdir(TRANSFER_ROOT, { recursive: true, mode: 0o700 });
};

const removeTransferFiles = async (transfer) => {
  if (!transfer?.dir) return;
  await fsp.rm(transfer.dir, { recursive: true, force: true });
};

const withTransferLock = async (transfer, task) => {
  const previous = transfer.lock || Promise.resolve();
  const next = previous.catch(() => {}).then(task);
  transfer.lock = next.catch(() => {});
  return next;
};

const streamRequestToFile = async (req, filePath, byteLimit) => {
  const hash = crypto.createHash('sha256');
  let bytes = 0;
  const meter = new Transform({
    transform(chunk, encoding, callback) {
      bytes += chunk.byteLength;
      if (bytes > byteLimit) {
        callback(httpError(413, 'Chunk exceeds the negotiated size limit.'));
        return;
      }
      hash.update(chunk);
      callback(null, chunk);
    },
  });

  await pipeline(req, meter, fs.createWriteStream(filePath, { flags: 'w' }));
  return { bytes, sha256: hash.digest('hex') };
};

const appendFile = async (sourcePath, targetPath) => {
  await pipeline(
    fs.createReadStream(sourcePath),
    fs.createWriteStream(targetPath, { flags: 'a' }),
  );
};

const hashFile = (filePath) =>
  new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const input = fs.createReadStream(filePath);
    input.on('data', (chunk) => hash.update(chunk));
    input.on('error', reject);
    input.on('end', () => resolve(hash.digest('hex')));
  });

const notifyTransferError = (transfer, message) => {
  const payload = {
    from: transfer.senderId,
    transferId: transfer.id,
    name: transfer.name,
    message,
    timestamp: Date.now(),
  };
  const sender = clients.get(transfer.senderId);
  const target = clients.get(transfer.targetId);
  if (sender) {
    send(sender.ws, 'large-file-error', {
      ...payload,
      targetId: transfer.targetId,
    });
  }
  if (target) {
    send(target.ws, 'large-file-error', {
      ...payload,
      displayName: sender?.displayName || transfer.senderId,
    });
  }
};

const failTransfer = (transfer, message) => {
  transfer.status = 'failed';
  transfer.updatedAt = Date.now();
  notifyTransferError(transfer, message);
  transfers.delete(transfer.id);
  removeTransferFiles(transfer).catch((error) => {
    console.warn(`Failed to remove transfer ${transfer.id}:`, error.message);
  });
};

const failTransfersForClient = (clientId, message) => {
  for (const transfer of transfers.values()) {
    if (
      transfer.status === 'uploading' &&
      (transfer.senderId === clientId || transfer.targetId === clientId)
    ) {
      failTransfer(transfer, message);
    }
  }
};

const cleanupExpiredTransfers = () => {
  const now = Date.now();
  for (const transfer of transfers.values()) {
    if (transfer.expiresAt > now) continue;
    transfers.delete(transfer.id);
    removeTransferFiles(transfer).catch((error) => {
      console.warn(`Failed to remove expired transfer ${transfer.id}:`, error.message);
    });
  }
};

const discoveryHostName = () => {
  const host = os.hostname().replace(/[^a-zA-Z0-9-]/g, '-').replace(/^-+|-+$/g, '');
  return host || 'snapsend';
};

const startLanDiscovery = () => {
  if (!ENABLE_MDNS) {
    console.log('局域网自发现已禁用（SNAPSEND_DISABLE_MDNS=1）。');
    return;
  }

  try {
    bonjour = new Bonjour({}, (error) => {
      console.warn(`局域网自发现异常：${error.message}`);
    });
    const name = `SnapSend ${discoveryHostName()}`;
    const txt = {
      app: 'snapsend',
      path: '/',
      protocol: 'http',
      version: '1',
    };
    bonjour.publish({
      name,
      type: 'snapsend',
      protocol: 'tcp',
      port: PORT,
      txt,
      disableIPv6: true,
    });
    bonjour.publish({
      name,
      type: 'http',
      protocol: 'tcp',
      port: PORT,
      txt,
      disableIPv6: true,
    });
    console.log('局域网自发现已启用：mDNS/Bonjour 服务 _snapsend._tcp 与 _http._tcp');
  } catch (error) {
    console.warn(`局域网自发现启动失败：${error.message}`);
  }
};

const stopLanDiscovery = () =>
  new Promise((resolve) => {
    if (!bonjour) {
      resolve();
      return;
    }
    bonjour.unpublishAll(() => {
      bonjour.destroy(() => {
        bonjour = null;
        resolve();
      });
    });
  });

const app = express();
const clientDir = path.resolve(__dirname, 'client');
const serveStatic = (fileName) => path.join(clientDir, fileName);

app.use(express.json({ limit: '256kb' }));
app.use(express.static(clientDir));

app.get('/', (req, res) => {
  res.sendFile(serveStatic('index.html'));
});

app.get('/index.html', (req, res) => {
  res.sendFile(serveStatic('index.html'));
});

app.get('/app.js', (req, res) => {
  res.sendFile(serveStatic('app.js'));
});

app.get('/styles.css', (req, res) => {
  res.sendFile(serveStatic('styles.css'));
});

app.get('/vendor/hash-wasm/index.esm.js', (req, res) => {
  res.sendFile(path.join(__dirname, 'node_modules/hash-wasm/dist/index.esm.js'));
});

app.post(
  '/api/transfers',
  asyncRoute(async (req, res) => {
    const { senderId, targetId, name, size, mime, chunkSize } = req.body || {};
    const sender = typeof senderId === 'string' ? clients.get(senderId) : null;
    const target = typeof targetId === 'string' ? clients.get(targetId) : null;
    const fileSize = parseSafeInteger(size);

    if (!sender) {
      throw httpError(403, 'Sender is not connected.');
    }
    if (!target) {
      throw httpError(404, 'Target device is unavailable.');
    }
    if (fileSize === null) {
      throw httpError(400, 'File size is invalid.');
    }

    await ensureTransferRoot();

    const transferId = uuid();
    const dir = path.join(TRANSFER_ROOT, transferId);
    const filePath = path.join(dir, 'payload.bin');
    const displayName = normalizeFileName(name);
    const transfer = {
      id: transferId,
      senderId,
      targetId,
      name: displayName,
      safeName: safeDownloadName(displayName),
      size: fileSize,
      mime: normalizeMime(mime),
      chunkSize: normalizeChunkSize(chunkSize),
      uploadToken: randomToken(),
      downloadToken: randomToken(),
      dir,
      filePath,
      receivedBytes: 0,
      nextIndex: 0,
      status: 'uploading',
      finalHash: '',
      createdAt: Date.now(),
      updatedAt: Date.now(),
      expiresAt: Date.now() + TRANSFER_TTL,
      lock: Promise.resolve(),
    };

    await fsp.mkdir(dir, { recursive: true, mode: 0o700 });
    await fsp.writeFile(filePath, '');
    transfers.set(transferId, transfer);

    send(target.ws, 'large-file-meta', {
      from: senderId,
      displayName: sender.displayName,
      transferId,
      name: transfer.name,
      size: transfer.size,
      mime: transfer.mime,
      timestamp: Date.now(),
    });

    res.status(201).json({
      transferId,
      uploadToken: transfer.uploadToken,
      chunkSize: transfer.chunkSize,
      expiresAt: transfer.expiresAt,
    });
  }),
);

app.put(
  '/api/transfers/:transferId/chunks/:index',
  asyncRoute(async (req, res) => {
    const transfer = transfers.get(req.params.transferId);
    if (!transfer) {
      throw httpError(404, 'Transfer was not found.');
    }

    await withTransferLock(transfer, async () => {
      if (transfer.status !== 'uploading') {
        throw httpError(409, 'Transfer is not accepting uploads.');
      }
      if (getTransferToken(req, 'x-upload-token') !== transfer.uploadToken) {
        throw httpError(403, 'Upload token is invalid.');
      }

      const index = parseSafeInteger(req.params.index);
      const chunkOffset = parseSafeInteger(req.get('x-chunk-offset'));
      const chunkSize = parseSafeInteger(req.get('x-chunk-size'));
      const contentLength = parseSafeInteger(req.get('content-length'));
      const expectedHash = String(req.get('x-chunk-sha256') || '').trim().toLowerCase();
      const remainingBytes = transfer.size - transfer.receivedBytes;
      const byteLimit = Math.min(transfer.chunkSize, remainingBytes);

      if (index === null || index !== transfer.nextIndex) {
        throw httpError(409, 'Unexpected chunk index.');
      }
      if (chunkOffset === null || chunkOffset !== transfer.receivedBytes) {
        throw httpError(409, 'Unexpected chunk offset.');
      }
      if (chunkSize === null || chunkSize <= 0 || chunkSize > byteLimit) {
        throw httpError(400, 'Chunk size is invalid.');
      }
      if (contentLength !== null && contentLength !== chunkSize) {
        throw httpError(400, 'Content-Length does not match the declared chunk size.');
      }
      if (!SHA256_HEX.test(expectedHash)) {
        throw httpError(400, 'Chunk SHA-256 is invalid.');
      }

      const chunkPath = path.join(transfer.dir, `chunk-${index}-${crypto.randomUUID()}.part`);
      let result;
      try {
        result = await streamRequestToFile(req, chunkPath, byteLimit);
        if (result.bytes !== chunkSize) {
          throw httpError(400, 'Uploaded chunk length does not match its metadata.');
        }
        if (result.sha256 !== expectedHash) {
          throw httpError(422, 'Chunk SHA-256 mismatch.');
        }
        await appendFile(chunkPath, transfer.filePath);
      } finally {
        await fsp.rm(chunkPath, { force: true }).catch(() => {});
      }

      transfer.receivedBytes += result.bytes;
      transfer.nextIndex += 1;
      transfer.updatedAt = Date.now();
      const percent = transfer.size
        ? Math.min(100, Math.round((transfer.receivedBytes / transfer.size) * 100))
        : 100;
      const target = clients.get(transfer.targetId);
      if (target) {
        send(target.ws, 'large-file-progress', {
          from: transfer.senderId,
          transferId: transfer.id,
          receivedBytes: transfer.receivedBytes,
          size: transfer.size,
          percent,
          timestamp: Date.now(),
        });
      }

      res.json({
        receivedBytes: transfer.receivedBytes,
        nextIndex: transfer.nextIndex,
        percent,
      });
    });
  }),
);

app.post(
  '/api/transfers/:transferId/complete',
  asyncRoute(async (req, res) => {
    const transfer = transfers.get(req.params.transferId);
    if (!transfer) {
      throw httpError(404, 'Transfer was not found.');
    }

    await withTransferLock(transfer, async () => {
      const { uploadToken, sha256, totalChunks } = req.body || {};
      const expectedHash = typeof sha256 === 'string' ? sha256.trim().toLowerCase() : '';
      const declaredChunks = parseSafeInteger(totalChunks);

      if (uploadToken !== transfer.uploadToken) {
        throw httpError(403, 'Upload token is invalid.');
      }
      if (transfer.status !== 'uploading') {
        throw httpError(409, 'Transfer is already finalized.');
      }
      if (!SHA256_HEX.test(expectedHash)) {
        throw httpError(400, 'File SHA-256 is invalid.');
      }
      if (declaredChunks === null || declaredChunks !== transfer.nextIndex) {
        throw httpError(409, 'Chunk count does not match the uploaded data.');
      }
      if (transfer.receivedBytes !== transfer.size) {
        throw httpError(409, 'Uploaded bytes do not match the file size.');
      }

      const serverHash = await hashFile(transfer.filePath);
      if (serverHash !== expectedHash) {
        failTransfer(transfer, '整文件 SHA-256 校验失败，传输已丢弃。');
        throw httpError(422, 'File SHA-256 mismatch.');
      }

      transfer.status = 'complete';
      transfer.finalHash = serverHash;
      transfer.updatedAt = Date.now();

      const sender = clients.get(transfer.senderId);
      const target = clients.get(transfer.targetId);
      const downloadUrl = `/api/transfers/${transfer.id}/download?token=${transfer.downloadToken}`;
      const payload = {
        from: transfer.senderId,
        displayName: sender?.displayName || transfer.senderId,
        transferId: transfer.id,
        name: transfer.name,
        size: transfer.size,
        mime: transfer.mime,
        sha256: transfer.finalHash,
        downloadUrl,
        expiresAt: transfer.expiresAt,
        timestamp: Date.now(),
      };

      if (target) {
        send(target.ws, 'large-file-ready', payload);
      }

      res.json({
        transferId: transfer.id,
        sha256: transfer.finalHash,
        downloadUrl,
        expiresAt: transfer.expiresAt,
      });
    });
  }),
);

app.delete(
  '/api/transfers/:transferId',
  asyncRoute(async (req, res) => {
    const transfer = transfers.get(req.params.transferId);
    if (!transfer) {
      res.status(204).end();
      return;
    }
    if (getTransferToken(req, 'x-upload-token') !== transfer.uploadToken) {
      throw httpError(403, 'Upload token is invalid.');
    }
    failTransfer(transfer, '发送方已取消传输。');
    res.status(204).end();
  }),
);

app.get(
  '/api/transfers/:transferId/download',
  asyncRoute(async (req, res) => {
    const transfer = transfers.get(req.params.transferId);
    if (!transfer || transfer.status !== 'complete') {
      throw httpError(404, 'Transfer is not available.');
    }
    if (getTransferToken(req, 'x-download-token') !== transfer.downloadToken) {
      throw httpError(403, 'Download token is invalid.');
    }

    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Content-SHA256', transfer.finalHash);
    res.setHeader('Content-Length', String(transfer.size));
    res.type(transfer.mime || 'application/octet-stream');
    res.download(transfer.filePath, transfer.safeName);
  }),
);

app.use((err, req, res, next) => {
  if (res.headersSent) {
    next(err);
    return;
  }
  const statusCode = err.statusCode || 500;
  if (statusCode >= 500) {
    console.error(err);
  }
  res.status(statusCode).json({
    error: err.message || 'Internal Server Error',
  });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: WS_MAX_PAYLOAD_BYTES });

const getPeerSnapshot = (excludeId) =>
  [...clients.entries()]
    .filter(([id]) => id !== excludeId)
    .map(([id, client]) => ({
      id,
      displayName: client.displayName,
      lastSeen: client.lastSeen,
    }));

const broadcast = (type, payload, exceptId) => {
  for (const [id, client] of clients.entries()) {
    if (id === exceptId) continue;
    send(client.ws, type, payload);
  }
};

wss.on('connection', (ws, req) => {
  const clientId = uuid();
  const clientRecord = {
    id: clientId,
    ws,
    displayName: generateUniqueDisplayName(),
    lastSeen: Date.now(),
    autoName: true,
  };
  clients.set(clientId, clientRecord);

  send(ws, 'welcome', {
    id: clientId,
    displayName: clientRecord.displayName,
    peers: getPeerSnapshot(clientId),
  });

  broadcast(
    'peer-joined',
    {
      id: clientId,
      displayName: clientRecord.displayName,
      lastSeen: clientRecord.lastSeen,
    },
    clientId,
  );

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      send(ws, 'error', { message: 'Invalid JSON payload.' });
      return;
    }

    const { type, payload } = data;
    clientRecord.lastSeen = Date.now();

    switch (type) {
      case 'register': {
        const { displayName } = payload || {};
        if (typeof displayName === 'string') {
          const trimmed = displayName.trim();
          if (trimmed) {
            if (clientRecord.autoName) {
              usedDisplayNames.delete(clientRecord.displayName);
              clientRecord.autoName = false;
            }
            clientRecord.displayName = trimmed.slice(0, 80);
          } else if (!clientRecord.autoName) {
            clientRecord.displayName = generateUniqueDisplayName();
            clientRecord.autoName = true;
          }
        }
        send(ws, 'registered', {
          id: clientId,
          displayName: clientRecord.displayName,
        });
        broadcast(
          'peer-updated',
          {
            id: clientId,
            displayName: clientRecord.displayName,
            lastSeen: clientRecord.lastSeen,
          },
          clientId,
        );
        break;
      }
      case 'signal': {
        const { targetId, data: signalData } = payload || {};
        if (!targetId || !signalData) break;
        const target = clients.get(targetId);
        if (!target) {
          send(ws, 'signal-error', { targetId, message: 'Target unavailable.' });
          break;
        }
        send(target.ws, 'signal', {
          from: clientId,
          data: signalData,
        });
        break;
      }
      case 'poke': {
        const { targetId } = payload || {};
        if (!targetId) break;
        const target = clients.get(targetId);
        if (!target) {
          send(ws, 'delivery-error', {
            targetId,
            message: 'Target unavailable.',
          });
          break;
        }
        send(target.ws, 'poke', {
          from: clientId,
          displayName: clientRecord.displayName,
          timestamp: Date.now(),
        });
        break;
      }
      case 'text-message': {
        const { message, targetId } = payload || {};
        if (typeof message !== 'string' || !message.trim()) break;
        if (targetId) {
          const target = clients.get(targetId);
          if (!target) {
            send(ws, 'delivery-error', {
              targetId,
              message: 'Target unavailable.',
            });
            break;
          }
          send(target.ws, 'text-message', {
            from: clientId,
            displayName: clientRecord.displayName,
            message,
            timestamp: Date.now(),
          });
        } else {
          broadcast(
            'text-message',
            {
              from: clientId,
              displayName: clientRecord.displayName,
              message,
              timestamp: Date.now(),
            },
            clientId,
          );
        }
        break;
      }
      case 'clipboard-update': {
        const { content, targetId, items } = payload || {};
        const packetPayload = {};
        if (typeof content === 'string') {
          packetPayload.content = content;
        }
        if (Array.isArray(items)) {
          const sanitizedItems = items
            .filter((entry) => entry && typeof entry.mime === 'string' && typeof entry.data === 'string')
            .map((entry) => ({
              mime: entry.mime,
              data: entry.data,
              encoding: entry.encoding === 'base64' ? 'base64' : 'text',
            }));
          if (sanitizedItems.length) {
            packetPayload.items = sanitizedItems;
          }
        }
        if (
          typeof packetPayload.content !== 'string' &&
          !Array.isArray(packetPayload.items)
        ) {
          break;
        }
        const packet = {
          from: clientId,
          displayName: clientRecord.displayName,
          timestamp: Date.now(),
          ...packetPayload,
        };
        if (targetId) {
          const target = clients.get(targetId);
          if (!target) {
            send(ws, 'delivery-error', {
              targetId,
              message: 'Target unavailable.',
            });
            break;
          }
          send(target.ws, 'clipboard-update', packet);
        } else {
          broadcast('clipboard-update', packet, clientId);
        }
        break;
      }
      case 'file-transfer-meta': {
        const { targetId, transferId, name, size, mime } = payload || {};
        if (!targetId || !transferId) break;
        const target = clients.get(targetId);
        if (!target) {
          send(ws, 'file-transfer-error', {
            transferId,
            targetId,
            message: 'Target unavailable.',
          });
          break;
        }
        send(target.ws, 'file-transfer-meta', {
          from: clientId,
          displayName: clientRecord.displayName,
          transferId,
          name,
          size,
          mime,
          timestamp: Date.now(),
        });
        break;
      }
      case 'file-transfer-chunk': {
        const { targetId, transferId, index, data, size } = payload || {};
        if (!targetId || !transferId || typeof data !== 'string') break;
        const target = clients.get(targetId);
        if (!target) {
          send(ws, 'file-transfer-error', {
            transferId,
            targetId,
            message: 'Target unavailable.',
          });
          break;
        }
        send(target.ws, 'file-transfer-chunk', {
          from: clientId,
          transferId,
          index,
          data,
          size,
        });
        break;
      }
      case 'file-transfer-complete': {
        const { targetId, transferId, name, mime } = payload || {};
        if (!targetId || !transferId) break;
        const target = clients.get(targetId);
        if (!target) {
          send(ws, 'file-transfer-error', {
            transferId,
            targetId,
            message: 'Target unavailable.',
          });
          break;
        }
        send(target.ws, 'file-transfer-complete', {
          from: clientId,
          transferId,
          name,
          mime,
          timestamp: Date.now(),
        });
        break;
      }
      case 'file-transfer-error': {
        const { targetId, transferId, message: errorMessage } = payload || {};
        if (!targetId || !transferId) break;
        const target = clients.get(targetId);
        if (target) {
          send(target.ws, 'file-transfer-error', {
            from: clientId,
            displayName: clientRecord.displayName,
            transferId,
            message: errorMessage,
          });
        }
        break;
      }
      case 'ping': {
        send(ws, 'pong', { timestamp: Date.now() });
        break;
      }
      case 'pong': {
        break;
      }
      default: {
        send(ws, 'error', { message: `Unrecognized message type: ${type}` });
      }
    }
  });

  ws.on('close', () => {
    if (clientRecord.autoName) {
      usedDisplayNames.delete(clientRecord.displayName);
    }
    failTransfersForClient(clientId, '设备已离线，传输已中止。');
    clients.delete(clientId);
    broadcast('peer-left', { id: clientId });
  });

  ws.on('error', () => {
    ws.close();
  });
});

setInterval(() => {
  const expiry = Date.now() - HEARTBEAT_INTERVAL * 2;
  for (const [id, client] of clients.entries()) {
    if (client.lastSeen < expiry) {
      client.ws.terminate();
      if (client.autoName) {
        usedDisplayNames.delete(client.displayName);
      }
      failTransfersForClient(id, '设备心跳超时，传输已中止。');
      clients.delete(id);
      broadcast('peer-left', { id });
    } else {
      send(client.ws, 'ping', { timestamp: Date.now() });
    }
  }
}, HEARTBEAT_INTERVAL);

setInterval(cleanupExpiredTransfers, TRANSFER_CLEANUP_INTERVAL).unref?.();

server.listen(PORT, () => {
  console.log(`SnapSend server listening on http://localhost:${PORT}`);
  const interfaces = os.networkInterfaces();
  const lanUrls = new Set();
  for (const infos of Object.values(interfaces)) {
    if (!infos) continue;
    for (const info of infos) {
      if (info.family === 'IPv4' && !info.internal && info.address) {
        lanUrls.add(`http://${info.address}:${PORT}`);
      }
    }
  }
  if (lanUrls.size > 0) {
    console.log('在本地网络访问:');
    for (const url of lanUrls) {
      console.log(`  • ${url}`);
    }
  }
  startLanDiscovery();
});

const shutdown = async () => {
  await stopLanDiscovery();
  server.close(() => {
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 1500).unref();
};

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
