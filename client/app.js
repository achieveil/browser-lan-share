import { createSHA256 } from './vendor/hash-wasm/index.esm.js';

const dom = {
  displayName: document.getElementById('displayName'),
  currentDisplayName: document.getElementById('currentDisplayName'),
  saveDisplayName: document.getElementById('saveDisplayName'),
  selfId: document.getElementById('selfId'),
  peerList: document.getElementById('peerList'),
  peerTemplate: document.getElementById('peerTemplate'),
  fileTarget: document.getElementById('fileTarget'),
  fileInput: document.getElementById('fileInput'),
  sendFileBtn: document.getElementById('sendFileBtn'),
  fileStatus: document.getElementById('fileStatus'),
  clipboardText: document.getElementById('clipboardText'),
  clipboardTarget: document.getElementById('clipboardTarget'),
  pushClipboardBtn: document.getElementById('pushClipboardBtn'),
  readAndBroadcastClipboardBtn: document.getElementById('readAndBroadcastClipboardBtn'),
  copyClipboardBtn: document.getElementById('copyClipboardBtn'),
  clipboardStatus: document.getElementById('clipboardStatus'),
  chatForm: document.getElementById('chatForm'),
  chatTarget: document.getElementById('chatTarget'),
  chatInput: document.getElementById('chatInput'),
  messages: document.getElementById('messages'),
  activityLog: document.getElementById('activityLog'),
  statusServer: document.getElementById('statusServer'),
  clipboardOverlay: document.getElementById('clipboardOverlay'),
  clipboardOverlayArea: document.getElementById('clipboardOverlayArea'),
  clipboardOverlayConfirm: document.getElementById('clipboardOverlayConfirm'),
  clipboardOverlayCancel: document.getElementById('clipboardOverlayCancel'),
  clipboardOverlayMessage: document.getElementById('clipboardOverlayMessage'),
  clipboardOverlayPreview: document.getElementById('clipboardOverlayPreview'),
  clipboardOverlayTitle: document.getElementById('clipboardOverlayTitle'),
  directSaveOverlay: document.getElementById('directSaveOverlay'),
  directSaveTitle: document.getElementById('directSaveTitle'),
  directSaveMessage: document.getElementById('directSaveMessage'),
  directSaveMeta: document.getElementById('directSaveMeta'),
  directSaveConfirm: document.getElementById('directSaveConfirm'),
  directSaveTemp: document.getElementById('directSaveTemp'),
  directSaveCancel: document.getElementById('directSaveCancel'),
};

const statusPanels = Array.from(document.querySelectorAll('.status-panel'));

const ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];
const FILE_CHUNK_SIZE = 64 * 1024;
const DIRECT_FILE_CHUNK_SIZE = 256 * 1024;
const DIRECT_MEMORY_LIMIT = 256 * 1024 * 1024;
const DIRECT_CONNECT_TIMEOUT = 8000;
const DIRECT_CONTROL_TIMEOUT = 15000;
const DIRECT_CHUNK_ACK_TIMEOUT = 30000;
const HTTP_FILE_CHUNK_SIZE = 4 * 1024 * 1024;
const WS_RECONNECT_BASE_DELAY = 1500;
const WS_RECONNECT_MAX_DELAY = 15000;
const SHORT_HASH_LENGTH = 12;

const state = {
  selfId: null,
  displayName: '',
  peers: new Map(),
  peerConnections: new Map(),
  incomingTransfers: new Map(),
  outgoingTransfers: new Map(),
  orphanCandidates: new Map(),
  transferTrackers: new Map(),
  hasSentInitialRegister: false,
  ws: null,
  wsReconnectTimer: null,
  wsReconnectAttempts: 0,
  lastClipboardPayload: null,
  clipboardTextLinkedPayload: null,
  directControlWaiters: new Map(),
  directChunkAckWaiters: new Map(),
};

const peerLabel = (peerId) => state.peers.get(peerId)?.displayName || peerId;

const deferred = () => {
  let settled = false;
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = (value) => {
      if (!settled) {
        settled = true;
        res(value);
      }
    };
    reject = (reason) => {
      if (!settled) {
        settled = true;
        rej(reason);
      }
    };
  });
  return { promise, resolve, reject };
};

const ACTIVITY_LIMIT = 200;
const ACTIVITY_LABELS = {
  outbound: '发送',
  inbound: '接收',
  status: '状态',
  warning: '提醒',
  error: '错误',
};
const SILENCED_OUTBOUND_TYPES = new Set(['pong', 'file-transfer-chunk']);
const SILENCED_INBOUND_TYPES = new Set(['pong', 'ping', 'file-transfer-chunk', 'large-file-progress']);

const truncate = (text, length = 80) => {
  if (typeof text !== 'string') return '';
  if (text.length <= length) return text;
  return `${text.slice(0, length)}…`;
};

const formatClock = () => new Date().toLocaleTimeString('zh-CN', { hour12: false });

const humanFileSize = (bytes) => {
  if (!Number.isFinite(bytes) || bytes < 0) return '';
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  const precision = size >= 10 || unitIndex === 0 ? 0 : 1;
  return `${size.toFixed(precision)} ${units[unitIndex]}`;
};

const withTimeout = (promise, ms, errorMessage) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(errorMessage instanceof Error ? errorMessage : new Error(errorMessage));
    }, ms);
    promise
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });

const readJsonResponse = async (response) => {
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(payload?.error || payload?.message || `请求失败：${response.status}`);
  }
  return payload || {};
};

const createSha256 = async () => {
  const hasher = await createSHA256();
  hasher.init();
  return hasher;
};

const hashBytes = async (hasher, bytes) => {
  hasher.init();
  hasher.update(bytes);
  return hasher.digest('hex');
};

const shortHash = (value) =>
  typeof value === 'string' && value
    ? `${value.slice(0, SHORT_HASH_LENGTH)}…`
    : '未知';

const dataChannelKey = (peerId) => `dc:${peerId}`;
const relayKey = (transferId) => `relay:${transferId}`;
const directKey = (transferId) => `direct:${transferId}`;
const directAckKey = (transferId, index) => `${transferId}:${index}`;

const createFallbackError = (message) => {
  const error = new Error(message);
  error.fallbackToRelay = true;
  return error;
};

const supportsOriginPrivateFileSystem = () =>
  Boolean(navigator.storage && typeof navigator.storage.getDirectory === 'function');

const supportsDirectSaveToDisk = () =>
  typeof window.showSaveFilePicker === 'function' &&
  typeof WritableStream !== 'undefined';

const bufferToBase64 = (buffer) => {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const chunkSize = 0x8000;
  let binary = '';
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
};

const base64ToUint8Array = (base64) => {
  const binary = atob(base64);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
};

const friendlyMimeLabel = (mime) => {
  if (!mime || typeof mime !== 'string') return '未知格式';
  if (mime === 'text/plain') return '纯文本';
  if (mime === 'text/html') return 'HTML';
  if (mime === 'text/rtf') return 'RTF';
  if (mime === 'text/uri-list') return '链接列表';
  if (mime.startsWith('image/')) {
    const subtype = mime.split('/')[1] || '';
    return `图像(${subtype.toUpperCase() || mime})`;
  }
  if (mime.startsWith('application/')) {
    return mime.split('/')[1]?.toUpperCase() || mime;
  }
  return mime;
};

const clipboardFormatHint = (descriptor) => {
  const items = Array.isArray(descriptor?.items) ? descriptor.items : [];
  if (!items.length) return '';
  const uniqueMimes = [...new Set(items.map((entry) => entry.mime).filter(Boolean))];
  if (!uniqueMimes.length) return '';
  const preview = uniqueMimes.slice(0, 3).map(friendlyMimeLabel).join('、');
  const suffix = uniqueMimes.length > 3 ? '等' : '';
  return `（含 ${uniqueMimes.length} 种格式：${preview}${suffix}）`;
};

const hasClipboardData = (descriptor) => {
  if (!descriptor) return false;
  const hasText = typeof descriptor.content === 'string' && descriptor.content.length > 0;
  const hasItems = Array.isArray(descriptor.items) && descriptor.items.length > 0;
  return hasText || hasItems;
};

const serializeClipboardItems = async (clipboardItems) => {
  const serialized = [];
  const items = Array.from(clipboardItems || []);
  const seenTypes = new Set();
  for (const item of items) {
    if (!item?.types) continue;
    for (const type of item.types) {
      if (!type || seenTypes.has(type)) continue;
      try {
        const blob = await item.getType(type);
        if (type.startsWith('text/') || type === 'application/json') {
          const text = await blob.text();
          serialized.push({ mime: type, encoding: 'text', data: text });
        } else {
          const buffer = await blob.arrayBuffer();
          serialized.push({ mime: type, encoding: 'base64', data: bufferToBase64(buffer) });
        }
        seenTypes.add(type);
      } catch (error) {
        console.warn('Failed to read clipboard item', type, error);
      }
    }
  }
  return serialized;
};

const extractPlainText = (descriptor, fallback = '') => {
  if (!descriptor) return fallback;
  if (typeof descriptor.content === 'string') return descriptor.content;
  if (Array.isArray(descriptor.items)) {
    const plainEntry = descriptor.items.find((entry) => entry.mime === 'text/plain');
    if (typeof plainEntry?.data === 'string') return plainEntry.data;
    const genericText = descriptor.items.find((entry) => entry.mime?.startsWith('text/'));
    if (typeof genericText?.data === 'string') return genericText.data;
  }
  return fallback;
};

const descriptorHasHtml = (descriptor) =>
  Array.isArray(descriptor?.items) &&
  descriptor.items.some((entry) => entry && entry.mime === 'text/html' && typeof entry.data === 'string');

const getDescriptorHtml = (descriptor) => {
  if (!Array.isArray(descriptor?.items)) return '';
  const entry = descriptor.items.find((item) => item && item.mime === 'text/html');
  if (!entry) return '';
  if (entry.encoding && entry.encoding !== 'text') {
    return '';
  }
  return typeof entry.data === 'string' ? entry.data : '';
};

const readClipboardWithExecCommand = () => {
  if (typeof document.execCommand !== 'function') {
    return null;
  }

  const selection = document.getSelection();
  const previousRanges = [];
  if (selection && selection.rangeCount) {
    for (let i = 0; i < selection.rangeCount; i += 1) {
      previousRanges.push(selection.getRangeAt(i));
    }
  }

  const activeElement = document.activeElement;

  const container = document.createElement('div');
  container.contentEditable = 'true';
  container.style.position = 'fixed';
  container.style.left = '-9999px';
  container.style.bottom = '0';
  container.style.pointerEvents = 'none';
  container.style.opacity = '0';
  container.style.whiteSpace = 'pre-wrap';
  document.body.appendChild(container);

  container.focus();

  let success = false;
  try {
    success = document.execCommand('paste');
  } catch {
    success = false;
  }

  const html = container.innerHTML;
  const text = container.textContent || '';

  if (selection) {
    selection.removeAllRanges();
    previousRanges.forEach((range) => selection.addRange(range));
  }

  if (activeElement && typeof activeElement.focus === 'function') {
    activeElement.focus();
  }

  container.remove();

  if (!success) {
    return null;
  }

  const items = [];
  if (html) {
    items.push({ mime: 'text/html', encoding: 'text', data: html });
  }
  if (text) {
    items.push({ mime: 'text/plain', encoding: 'text', data: text });
  }

  return {
    content: text,
    items,
  };
};

const descriptorFromClipboardData = (clipboardData) => {
  if (!clipboardData) return null;
  const entries = new Map();
  const store = (mime, data) => {
    if (typeof data !== 'string' || !data) return;
    entries.set(mime, { mime, encoding: 'text', data });
  };

  const hasGetData = typeof clipboardData.getData === 'function';
  const knownTypes = hasGetData ? Array.from(clipboardData.types || []) : [];
  if (hasGetData) {
    if (knownTypes.includes('text/html') || knownTypes.includes('text/HTML')) {
      store('text/html', clipboardData.getData('text/html') || clipboardData.getData('text/HTML'));
    }
    if (knownTypes.includes('text/rtf')) {
      store('text/rtf', clipboardData.getData('text/rtf'));
    }
    if (knownTypes.includes('text/uri-list')) {
      store('text/uri-list', clipboardData.getData('text/uri-list'));
    }
    store('text/plain', clipboardData.getData('text/plain'));
  }

  const items = Array.from(entries.values());
  const content = entries.get('text/plain')?.data || extractPlainText({ items }, '');
  const descriptor = { content: content || '' };
  if (items.length) {
    descriptor.items = items;
  }
  return descriptor;
};

const descriptorFormatLabels = (descriptor) => {
  if (!descriptor) return [];
  const formats = new Set();
  if (Array.isArray(descriptor.items)) {
    descriptor.items.forEach((entry) => {
      if (entry?.mime) {
        formats.add(friendlyMimeLabel(entry.mime));
      }
    });
  }
  if (typeof descriptor.content === 'string' && descriptor.content.length > 0) {
    formats.add(friendlyMimeLabel('text/plain'));
  }
  return Array.from(formats);
};

const descriptorFormatSummary = (descriptor) => {
  const formats = descriptorFormatLabels(descriptor);
  if (!formats.length) {
    return '尚未粘贴内容。';
  }
  return `捕获格式：${formats.join('、')}`;
};

const overlayState = {
  active: false,
  descriptor: null,
  resolve: null,
  restoreFocus: null,
  cleanup: null,
};

const closeClipboardOverlay = (result) => {
  if (!dom.clipboardOverlay) return;
  if (typeof overlayState.cleanup === 'function') {
    overlayState.cleanup();
  }
  dom.clipboardOverlay.hidden = true;
  dom.clipboardOverlayArea?.removeAttribute('data-overlay-listening');
  dom.clipboardOverlayArea?.replaceChildren();
  if (dom.clipboardOverlayPreview) {
    dom.clipboardOverlayPreview.textContent = '';
  }
  if (overlayState.restoreFocus && typeof overlayState.restoreFocus.focus === 'function') {
    overlayState.restoreFocus.focus();
  }
  const resolver = overlayState.resolve;
  overlayState.active = false;
  overlayState.descriptor = null;
  overlayState.resolve = null;
  overlayState.restoreFocus = null;
  overlayState.cleanup = null;
  if (resolver) {
    resolver(result || null);
  }
};

const openClipboardOverlay = (options = {}) => {
  if (!dom.clipboardOverlay || !dom.clipboardOverlayArea) {
    return Promise.resolve(null);
  }
  if (overlayState.active) {
    closeClipboardOverlay(null);
  }
  return new Promise((resolve) => {
    overlayState.active = true;
    overlayState.resolve = resolve;
    overlayState.descriptor = null;
    overlayState.restoreFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;

    const {
      title = '粘贴剪贴板内容',
      message = '浏览器阻止直接读取剪贴板，请在下方区域按 Ctrl+V / ⌘V 粘贴内容。',
      confirmLabel = '确定',
    } = options;

    if (dom.clipboardOverlayTitle) {
      dom.clipboardOverlayTitle.textContent = title;
    }
    if (dom.clipboardOverlayMessage) {
      dom.clipboardOverlayMessage.textContent = message;
    }
    if (dom.clipboardOverlayConfirm) {
      dom.clipboardOverlayConfirm.textContent = confirmLabel;
    }
    if (dom.clipboardOverlayPreview) {
      dom.clipboardOverlayPreview.textContent = '尚未粘贴内容。';
    }

    dom.clipboardOverlay.hidden = false;
    dom.clipboardOverlayArea.innerHTML = '';
    const area = dom.clipboardOverlayArea;
    area.setAttribute('data-overlay-listening', 'true');

    const updatePreview = () => {
      if (!dom.clipboardOverlayPreview) return;
      dom.clipboardOverlayPreview.textContent = descriptorFormatSummary(overlayState.descriptor);
    };

    const handlePaste = (event) => {
      event.preventDefault();
      const descriptor = descriptorFromClipboardData(event.clipboardData);
      overlayState.descriptor = descriptor;
      const html = descriptorHasHtml(descriptor) ? getDescriptorHtml(descriptor) : '';
      if (html) {
        area.innerHTML = html;
      } else if (typeof descriptor?.content === 'string') {
        area.textContent = descriptor.content;
      } else {
        area.textContent = '';
      }
      updatePreview();
    };

    const handleInput = () => {
      overlayState.descriptor = null;
      updatePreview();
    };

    const handleConfirm = () => {
      let descriptor = overlayState.descriptor;
      if (!hasClipboardData(descriptor)) {
        const manualText = area.innerText || area.textContent || '';
        if (manualText.trim()) {
          descriptor = { content: manualText };
        }
      }
      closeClipboardOverlay(hasClipboardData(descriptor) ? normalizeClipboardDescriptor(descriptor) : null);
    };

    const handleCancel = () => {
      closeClipboardOverlay(null);
    };

    const handleKeydown = (event) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        handleCancel();
      } else if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        handleConfirm();
      }
    };

    const detach = () => {
      area.removeEventListener('paste', handlePaste);
      area.removeEventListener('input', handleInput);
      dom.clipboardOverlayConfirm?.removeEventListener('click', handleConfirm);
      dom.clipboardOverlayCancel?.removeEventListener('click', handleCancel);
      dom.clipboardOverlay?.removeEventListener('keydown', handleKeydown, true);
    };

    area.addEventListener('paste', handlePaste);
    area.addEventListener('input', handleInput);
    dom.clipboardOverlayConfirm?.addEventListener('click', handleConfirm);
    dom.clipboardOverlayCancel?.addEventListener('click', handleCancel);
    dom.clipboardOverlay?.addEventListener('keydown', handleKeydown, true);

    overlayState.cleanup = detach;

    setTimeout(() => {
      area.focus();
      updatePreview();
    }, 0);
  });
};

const readSystemClipboardDescriptor = async () => {
  if (!navigator.clipboard) {
    const commandDescriptor = readClipboardWithExecCommand();
    if (commandDescriptor) {
      return { descriptor: commandDescriptor, mode: 'command' };
    }
    throw new Error('当前环境不支持剪贴板 API。');
  }
  let richError = null;
  if (
    typeof navigator.clipboard.read === 'function' &&
    typeof ClipboardItem !== 'undefined'
  ) {
    try {
      const items = await navigator.clipboard.read();
      const serialized = await serializeClipboardItems(items);
      const descriptor = {
        content: extractPlainText({ items: serialized }, ''),
        items: serialized,
      };
      return { descriptor: normalizeClipboardDescriptor(descriptor), mode: 'rich' };
    } catch (error) {
      richError = error;
    }
  }
  const commandDescriptor = readClipboardWithExecCommand();
  if (commandDescriptor) {
    return { descriptor: normalizeClipboardDescriptor(commandDescriptor), mode: 'command' };
  }
  if (typeof navigator.clipboard.readText === 'function') {
    try {
      const text = await navigator.clipboard.readText();
      return { descriptor: normalizeClipboardDescriptor({ content: text || '' }), mode: 'text' };
    } catch (error) {
      if (!richError) richError = error;
    }
  }
  if (dom.clipboardOverlay) {
    const manualDescriptor = await openClipboardOverlay({
      message: '浏览器阻止直接读取剪贴板，请在下方区域按 Ctrl+V / ⌘V 粘贴内容，我们会保留原始格式。',
      confirmLabel: '使用此内容',
    });
    if (manualDescriptor && hasClipboardData(manualDescriptor)) {
      return { descriptor: normalizeClipboardDescriptor(manualDescriptor), mode: 'manual' };
    }
  }
  if (richError) throw richError;
  return { descriptor: null, mode: 'unsupported' };
};

const writeClipboardData = async (descriptor, options = {}) => {
  const { allowTextFallback = true } = options;
  if (!navigator.clipboard) {
    throw new Error('当前环境不支持剪贴板写入。');
  }
  if (
    descriptor &&
    Array.isArray(descriptor.items) &&
    descriptor.items.length > 0 &&
    typeof navigator.clipboard.write === 'function' &&
    typeof ClipboardItem !== 'undefined'
  ) {
    const itemEntries = {};
    let hasBlob = false;
    for (const entry of descriptor.items) {
      if (!entry?.mime || !entry?.data) continue;
      try {
        if (entry.encoding === 'base64') {
          const bytes = base64ToUint8Array(entry.data);
          itemEntries[entry.mime] = new Blob([bytes], { type: entry.mime });
        } else {
          itemEntries[entry.mime] = new Blob([entry.data], { type: entry.mime });
        }
        hasBlob = true;
      } catch (error) {
        console.warn('Failed to reconstruct clipboard item', entry.mime, error);
      }
    }
    if (
      typeof descriptor.content === 'string' &&
      descriptor.content &&
      !itemEntries['text/plain']
    ) {
      itemEntries['text/plain'] = new Blob([descriptor.content], { type: 'text/plain' });
      hasBlob = true;
    }
    if (hasBlob) {
      try {
        await navigator.clipboard.write([new ClipboardItem(itemEntries)]);
        return { mode: 'rich', fallback: false };
      } catch (error) {
        if (!allowTextFallback) {
          throw error;
        }
        if (typeof descriptor.content !== 'string' || typeof navigator.clipboard.writeText !== 'function') {
          throw error;
        }
        await navigator.clipboard.writeText(descriptor.content);
        return { mode: 'text', fallback: true };
      }
    }
  }
  const text =
    typeof descriptor === 'string'
      ? descriptor
      : typeof descriptor?.content === 'string'
      ? descriptor.content
      : '';
  if (typeof navigator.clipboard.writeText !== 'function') {
    throw new Error('此浏览器不支持写入纯文本剪贴板。');
  }
  if (!allowTextFallback) {
    throw new Error('此浏览器不支持写入纯文本剪贴板。');
  }
  if (!text) {
    throw new Error('剪贴板内容为空或无法转换为纯文本。');
  }
  await navigator.clipboard.writeText(text);
  return { mode: 'text', fallback: false };
};

const normalizeClipboardDescriptor = (descriptor) => {
  if (!descriptor || typeof descriptor !== 'object') {
    return {};
  }
  const result = {};
  if (typeof descriptor.content === 'string') {
    result.content = descriptor.content;
  }
  if (Array.isArray(descriptor.items)) {
    const items = descriptor.items
      .filter((entry) => entry && typeof entry.mime === 'string' && typeof entry.data === 'string')
      .map((entry) => ({
        mime: entry.mime,
        data: entry.data,
        encoding: entry.encoding === 'base64' ? 'base64' : 'text',
      }));
    if (items.length) {
      result.items = items;
    }
  }
  return result;
};

const microDelay = () => new Promise((resolve) => setTimeout(resolve, 0));

const updateStatusPanelScroll = (container) => {
  const panel = container ? container.closest('.status-panel') : null;
  if (!container) return;
  if (panel) {
    if (panel.open) {
      container.scrollTop = container.scrollHeight;
      panel.classList.remove('has-updates');
    } else {
      panel.classList.add('has-updates');
    }
  } else {
    container.scrollTop = container.scrollHeight;
  }
};

const createTransferDisplay = ({ direction, peerId, name, size }) => {
  if (!dom.fileStatus) return null;
  const entry = document.createElement('div');
  entry.className = `transfer-entry transfer-${direction}`;

  const heading = document.createElement('div');
  heading.className = 'transfer-heading';
  heading.textContent =
    direction === 'outbound'
      ? `发送到 ${peerLabel(peerId)}`
      : `来自 ${peerLabel(peerId)}`;

  const filename = document.createElement('div');
  filename.className = 'transfer-filename';
  filename.textContent = name || '未命名文件';

  const meta = document.createElement('div');
  meta.className = 'transfer-meta';
  meta.textContent = humanFileSize(size);

  const progress = document.createElement('progress');
  progress.max = 100;
  progress.value = 0;

  const status = document.createElement('div');
  status.className = 'transfer-status-line';
  status.textContent = direction === 'outbound' ? '正在建立连接…' : '等待数据…';

  entry.append(heading, filename, meta, progress, status);
  dom.fileStatus.appendChild(entry);
  updateStatusPanelScroll(dom.fileStatus);
  return {
    entry,
    progress,
    status,
    meta,
    heading,
    filename,
  };
};

const updateTransferDisplay = (display, { percent, status }) => {
  if (!display) return;
  if (typeof percent === 'number' && !Number.isNaN(percent)) {
    display.progress.value = Math.max(0, Math.min(100, percent));
  }
  if (status) {
    display.status.textContent = status;
  }
};

const completeTransferDisplay = (display, message) => {
  if (!display) return;
  display.entry.classList.add('transfer-complete');
  display.progress.value = 100;
  if (message) {
    display.status.textContent = message;
  }
};

const failTransferDisplay = (display, message) => {
  if (!display) return;
  display.entry.classList.add('transfer-error');
  if (message) {
    display.status.textContent = message;
  }
};

const triggerDownload = (blob, filename) => {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename || '接收文件';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  return url;
};

const triggerDownloadUrl = (url, filename) => {
  const link = document.createElement('a');
  link.href = url;
  link.download = filename || '接收文件';
  link.rel = 'noopener';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};

const supportsVerifiedStreamingDownload = () =>
  typeof window.showSaveFilePicker === 'function' &&
  typeof ReadableStream !== 'undefined' &&
  typeof WritableStream !== 'undefined';

const downloadWithVerification = async (payload, display) => {
  if (!supportsVerifiedStreamingDownload()) {
    throw new Error('当前浏览器不支持流式校验保存。');
  }

  const filename = payload.name || '接收文件';
  let fileHandle;
  try {
    fileHandle = await window.showSaveFilePicker({
      suggestedName: filename,
    });
  } catch (error) {
    if (error?.name === 'AbortError') {
      updateTransferDisplay(display, {
        status: '已取消选择保存位置。',
      });
      return;
    }
    throw error;
  }

  const response = await fetch(payload.downloadUrl, { cache: 'no-store' });
  if (!response.ok || !response.body) {
    throw new Error(`下载请求失败：${response.status}`);
  }

  const writable = await fileHandle.createWritable();
  const reader = response.body.getReader();
  const hasher = await createSha256();
  const expectedSize = payload.size || Number(response.headers.get('content-length')) || 0;
  let receivedBytes = 0;
  let closed = false;

  try {
    while (true) {
      // eslint-disable-next-line no-await-in-loop
      const { done, value } = await reader.read();
      if (done) break;
      hasher.update(value);
      // eslint-disable-next-line no-await-in-loop
      await writable.write(value);
      receivedBytes += value.byteLength;
      const percent = expectedSize
        ? Math.min(100, Math.round((receivedBytes / expectedSize) * 100))
        : 0;
      updateTransferDisplay(display, {
        percent,
        status: `正在保存并校验 ${percent}%`,
      });
    }

    const digest = hasher.digest('hex');
    if (payload.sha256 && digest !== payload.sha256) {
      throw new Error('下载后的 SHA-256 与发送端不一致。');
    }
    await writable.close();
    closed = true;
    completeTransferDisplay(display, `已下载并校验 SHA-256：${shortHash(digest)}`);
    appendStatus(dom.fileStatus, `文件 "${filename}" 已保存，SHA-256 校验通过。`);
  } finally {
    if (!closed) {
      await writable.abort().catch(() => {});
    }
  }
};

const appendDownloadActions = (display, payload) => {
  const filename = payload.name || '接收文件';
  const actions = document.createElement('div');
  actions.className = 'transfer-actions';

  const nativeLink = document.createElement('a');
  nativeLink.href = payload.downloadUrl;
  nativeLink.download = filename;
  nativeLink.textContent = `下载 ${filename}`;
  nativeLink.className = 'download-link';
  nativeLink.rel = 'noopener';
  actions.appendChild(nativeLink);

  if (supportsVerifiedStreamingDownload()) {
    const verifiedButton = document.createElement('button');
    verifiedButton.type = 'button';
    verifiedButton.className = 'secondary-action';
    verifiedButton.textContent = '校验下载';
    verifiedButton.addEventListener('click', async () => {
      verifiedButton.disabled = true;
      try {
        await downloadWithVerification(payload, display);
      } catch (error) {
        failTransferDisplay(display, `校验下载失败：${error.message}`);
        appendStatus(dom.fileStatus, `校验下载失败：${error.message}`);
      } finally {
        verifiedButton.disabled = false;
      }
    });
    actions.appendChild(verifiedButton);
  }

  if (display) {
    display.entry.appendChild(actions);
    updateStatusPanelScroll(dom.fileStatus);
  } else if (dom.fileStatus) {
    dom.fileStatus.appendChild(actions);
    updateStatusPanelScroll(dom.fileStatus);
  }

  const expiresIn = payload.expiresAt
    ? Math.max(0, payload.expiresAt - Date.now())
    : 2 * 60 * 60 * 1000;
  setTimeout(() => {
    if (nativeLink.isConnected) {
      nativeLink.textContent = `${filename} 下载链接已过期`;
      nativeLink.removeAttribute('href');
      nativeLink.classList.add('download-link-disabled');
    }
  }, Math.min(expiresIn, 2 ** 31 - 1));
};

const sendDataChannelMessage = (channel, message) => {
  if (!channel || channel.readyState !== 'open') {
    throw createFallbackError('局域网直连通道尚未打开。');
  }
  channel.send(JSON.stringify(message));
};

const waitForDirectControl = (transferId, expectedTypes, peerId, timeout = DIRECT_CONTROL_TIMEOUT) =>
  new Promise((resolve, reject) => {
    const expected = new Set(Array.isArray(expectedTypes) ? expectedTypes : [expectedTypes]);
    const timer = setTimeout(() => {
      state.directControlWaiters.delete(transferId);
      reject(createFallbackError('等待直连响应超时。'));
    }, timeout);
    state.directControlWaiters.set(transferId, {
      peerId,
      expected,
      resolve: (message) => {
        clearTimeout(timer);
        resolve(message);
      },
      reject: (error) => {
        clearTimeout(timer);
        reject(error);
      },
    });
  });

const settleDirectControl = (message) => {
  const transferId = message.id || message.transferId;
  if (!transferId) return false;
  const waiter = state.directControlWaiters.get(transferId);
  if (!waiter || !waiter.expected.has(message.type)) return false;
  state.directControlWaiters.delete(transferId);
  waiter.resolve(message);
  return true;
};

const waitForDirectChunkAck = (transferId, index, peerId) =>
  new Promise((resolve, reject) => {
    const key = directAckKey(transferId, index);
    const timer = setTimeout(() => {
      state.directChunkAckWaiters.delete(key);
      reject(createFallbackError('等待直连分片确认超时。'));
    }, DIRECT_CHUNK_ACK_TIMEOUT);
    state.directChunkAckWaiters.set(key, {
      peerId,
      resolve: (message) => {
        clearTimeout(timer);
        resolve(message);
      },
      reject: (error) => {
        clearTimeout(timer);
        reject(error);
      },
    });
  });

const settleDirectChunkAck = (message) => {
  const key = directAckKey(message.id, message.index);
  const waiter = state.directChunkAckWaiters.get(key);
  if (!waiter) return false;
  state.directChunkAckWaiters.delete(key);
  waiter.resolve(message);
  return true;
};

const rejectDirectWaiters = (peerId, error) => {
  for (const [transferId, waiter] of state.directControlWaiters.entries()) {
    if (waiter.peerId !== peerId) continue;
    state.directControlWaiters.delete(transferId);
    waiter.reject(error);
  }
  for (const [key, waiter] of state.directChunkAckWaiters.entries()) {
    if (waiter.peerId !== peerId) continue;
    state.directChunkAckWaiters.delete(key);
    waiter.reject(error);
  }
};

const rejectDirectWaitersForTransfer = (transferId, error) => {
  const controlWaiter = state.directControlWaiters.get(transferId);
  if (controlWaiter) {
    state.directControlWaiters.delete(transferId);
    controlWaiter.reject(error);
  }
  for (const [key, waiter] of state.directChunkAckWaiters.entries()) {
    if (!key.startsWith(`${transferId}:`)) continue;
    state.directChunkAckWaiters.delete(key);
    waiter.reject(error);
  }
};

const directSaveOverlayState = {
  active: false,
  resolve: null,
  cleanup: null,
  restoreFocus: null,
};

const closeDirectSaveOverlay = (result) => {
  if (!dom.directSaveOverlay) return;
  if (typeof directSaveOverlayState.cleanup === 'function') {
    directSaveOverlayState.cleanup();
  }
  dom.directSaveOverlay.hidden = true;
  if (dom.directSaveMeta) {
    dom.directSaveMeta.textContent = '';
  }
  if (dom.directSaveMessage) {
    dom.directSaveMessage.textContent = '';
  }
  if (directSaveOverlayState.restoreFocus && typeof directSaveOverlayState.restoreFocus.focus === 'function') {
    directSaveOverlayState.restoreFocus.focus();
  }
  const resolver = directSaveOverlayState.resolve;
  directSaveOverlayState.active = false;
  directSaveOverlayState.resolve = null;
  directSaveOverlayState.cleanup = null;
  directSaveOverlayState.restoreFocus = null;
  if (resolver) {
    resolver(result || { action: 'reject', message: '接收方取消了文件接收。' });
  }
};

const openDirectSaveOverlay = ({ peerName, name, size, mime }) => {
  if (!dom.directSaveOverlay) {
    return Promise.resolve({ action: 'temp' });
  }
  if (directSaveOverlayState.active) {
    closeDirectSaveOverlay({
      action: 'reject',
      message: '已有文件接收确认正在处理。',
    });
  }

  return new Promise((resolve) => {
    directSaveOverlayState.active = true;
    directSaveOverlayState.resolve = resolve;
    directSaveOverlayState.restoreFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;

    if (dom.directSaveTitle) {
      dom.directSaveTitle.textContent = '接收文件';
    }
    if (dom.directSaveMessage) {
      dom.directSaveMessage.textContent = `${peerName} 正在通过局域网直连发送文件。`;
    }
    if (dom.directSaveMeta) {
      const format = mime ? ` · ${mime}` : '';
      dom.directSaveMeta.textContent = `${name || '未命名文件'} · ${humanFileSize(size)}${format}`;
    }
    if (dom.directSaveConfirm) {
      dom.directSaveConfirm.disabled = !supportsDirectSaveToDisk();
      dom.directSaveConfirm.textContent = supportsDirectSaveToDisk() ? '实时保存' : '不支持实时保存';
      dom.directSaveConfirm.title = supportsDirectSaveToDisk()
        ? '选择保存位置后，数据到达时会直接写入该文件'
        : '当前浏览器不支持 File System Access API';
    }

    const handleSave = async () => {
      if (!supportsDirectSaveToDisk()) {
        if (dom.directSaveMessage) {
          dom.directSaveMessage.textContent = '当前浏览器不支持实时保存，请选择临时接收或等待服务器中转。';
        }
        return;
      }
      try {
        const fileHandle = await window.showSaveFilePicker({
          suggestedName: name || '接收文件',
        });
        const writable = await fileHandle.createWritable();
        closeDirectSaveOverlay({
          action: 'save',
          storage: 'direct-save',
          fileHandle,
          writable,
        });
      } catch (error) {
        if (error?.name === 'AbortError') {
          if (dom.directSaveMessage) {
            dom.directSaveMessage.textContent = '未选择保存位置，可以重新选择、临时接收或拒绝。';
          }
          return;
        }
        if (dom.directSaveMessage) {
          dom.directSaveMessage.textContent = `打开保存位置失败：${error.message}`;
        }
      }
    };

    const handleTemp = () => {
      closeDirectSaveOverlay({ action: 'temp' });
    };

    const handleCancel = () => {
      closeDirectSaveOverlay({
        action: 'reject',
        message: '接收方拒绝了文件接收。',
      });
    };

    const handleKeydown = (event) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        handleCancel();
      }
    };

    const detach = () => {
      dom.directSaveConfirm?.removeEventListener('click', handleSave);
      dom.directSaveTemp?.removeEventListener('click', handleTemp);
      dom.directSaveCancel?.removeEventListener('click', handleCancel);
      dom.directSaveOverlay?.removeEventListener('keydown', handleKeydown, true);
    };

    dom.directSaveConfirm?.addEventListener('click', handleSave);
    dom.directSaveTemp?.addEventListener('click', handleTemp);
    dom.directSaveCancel?.addEventListener('click', handleCancel);
    dom.directSaveOverlay?.addEventListener('keydown', handleKeydown, true);
    directSaveOverlayState.cleanup = detach;
    dom.directSaveOverlay.hidden = false;

    setTimeout(() => {
      if (supportsDirectSaveToDisk()) {
        dom.directSaveConfirm?.focus();
      } else {
        dom.directSaveTemp?.focus();
      }
    }, 0);
  });
};

const cleanupDirectTransferStorage = async (transfer) => {
  if (!transfer) return;
  if (transfer.writable && !transfer.storageClosed) {
    await transfer.writable.abort().catch(() => {});
    transfer.storageClosed = true;
  }
  if (transfer.opfsRoot && transfer.opfsName) {
    await transfer.opfsRoot.removeEntry(transfer.opfsName).catch(() => {});
  }
};

const createTemporaryDirectReceiveStorage = async (transferId, size) => {
  if (supportsOriginPrivateFileSystem()) {
    try {
      const root = await navigator.storage.getDirectory();
      const opfsName = `snapsend-${transferId}.part`;
      const fileHandle = await root.getFileHandle(opfsName, { create: true });
      const writable = await fileHandle.createWritable();
      return {
        storage: 'opfs',
        fileHandle,
        writable,
        opfsRoot: root,
        opfsName,
      };
    } catch (error) {
      appendStatus(dom.fileStatus, `无法启用浏览器临时文件存储，准备检查内存接收能力：${error.message}`);
    }
  }

  if (size <= DIRECT_MEMORY_LIMIT) {
    return {
      storage: 'memory',
      chunks: [],
    };
  }

  return null;
};

const createDirectReceiveStorage = async ({ transferId, peerId, name, size, mime }) => {
  const choice = await openDirectSaveOverlay({
    peerName: peerLabel(peerId),
    name,
    size,
    mime,
  });

  if (choice.action === 'reject') {
    return {
      rejected: true,
      message: choice.message || '接收方拒绝了文件接收。',
    };
  }

  if (choice.action === 'save') {
    return {
      storage: 'direct-save',
      fileHandle: choice.fileHandle,
      writable: choice.writable,
    };
  }

  const temporaryStorage = await createTemporaryDirectReceiveStorage(transferId, size);
  if (temporaryStorage) {
    return temporaryStorage;
  }

  return {
    rejected: true,
    message: `当前浏览器无法安全直连接收 ${humanFileSize(size)}，请使用服务器中转。`,
  };
};

const finalizeDirectDownload = async (transfer, sha256) => {
  const filename = transfer.meta.name || '接收文件';
  let blob;
  if (transfer.storage === 'direct-save') {
    await transfer.writable.close();
    transfer.storageClosed = true;
    if (transfer.display) {
      completeTransferDisplay(transfer.display, `已实时保存到本地文件，SHA-256：${shortHash(sha256)}`);
      updateStatusPanelScroll(dom.fileStatus);
    }
    appendStatus(dom.fileStatus, `已通过局域网直连实时保存 "${filename}"，SHA-256 校验通过。`);
    return;
  }

  if (transfer.storage === 'opfs') {
    await transfer.writable.close();
    transfer.storageClosed = true;
    blob = await transfer.fileHandle.getFile();
  } else {
    blob = new Blob(transfer.chunks, {
      type: transfer.meta.mime || 'application/octet-stream',
    });
  }

  const downloadUrl = triggerDownload(blob, filename);
  const link = document.createElement('a');
  link.href = downloadUrl;
  link.download = filename;
  link.textContent = `重新下载 ${filename}`;
  link.className = 'download-link';
  link.rel = 'noopener';
  const actions = document.createElement('div');
  actions.className = 'transfer-actions';
  actions.appendChild(link);
  if (transfer.display) {
    completeTransferDisplay(transfer.display, `直连接收完成，SHA-256：${shortHash(sha256)}`);
    transfer.display.entry.appendChild(actions);
    updateStatusPanelScroll(dom.fileStatus);
  }
  appendStatus(dom.fileStatus, `已通过局域网直连接收 "${filename}"，SHA-256 校验通过。`);

  setTimeout(() => {
    URL.revokeObjectURL(downloadUrl);
    if (link.isConnected) {
      link.textContent = `${filename} 下载链接已过期`;
      link.removeAttribute('href');
      link.classList.add('download-link-disabled');
    }
    if (transfer.storage === 'opfs') {
      cleanupDirectTransferStorage(transfer);
    }
  }, 5 * 60 * 1000);
};

const setServerStatus = (text) => {
  if (dom.statusServer) {
    dom.statusServer.textContent = text;
  }
};


const appendActivity = (kind, description) => {
  if (!dom.activityLog) return;
  const entry = document.createElement('div');
  entry.className = `activity-entry activity-${kind}`;
  const timestamp = formatClock();
  const label = document.createElement('strong');
  label.textContent = `[${timestamp}] ${ACTIVITY_LABELS[kind] || ACTIVITY_LABELS.status}`;
  const text = document.createElement('span');
  text.textContent = description;
  entry.append(label, text);
  dom.activityLog.appendChild(entry);
  while (dom.activityLog.children.length > ACTIVITY_LIMIT) {
    dom.activityLog.removeChild(dom.activityLog.firstChild);
  }
  dom.activityLog.scrollTop = dom.activityLog.scrollHeight;
};

const describeOutbound = (type, payload = {}) => {
  switch (type) {
    case 'text-message': {
      const target = payload.targetId ? `向 ${payload.targetId}` : '向所有设备';
      const preview = payload.message ? `：${truncate(payload.message)}` : '';
      return `${target}发送消息${preview}`;
    }
    case 'clipboard-update': {
      const target = payload.targetId ? `向 ${payload.targetId}` : '向所有设备';
      const preview = payload.content ? `：${truncate(payload.content)}` : '';
      return `${target}同步剪贴板${preview}`;
    }
    case 'register':
      return payload.displayName
        ? `更新设备名称为「${payload.displayName}」`
        : '更新设备名称';
    case 'signal': {
      const kind = payload.data?.type || 'signal';
      const target = payload.targetId ? ` → ${payload.targetId}` : '';
      return `发送 ${kind} 信令${target}`;
    }
    case 'poke': {
      const target = payload.targetId ? peerLabel(payload.targetId) : '未知设备';
      return `我拍了拍 ${target}`;
    }
    case 'file-transfer-meta': {
      const target = payload.targetId ? peerLabel(payload.targetId) : '未知设备';
      return `准备通过服务器向 ${target} 发送文件「${payload.name || '未命名文件'}」`;
    }
    case 'file-transfer-complete': {
      const target = payload.targetId ? peerLabel(payload.targetId) : '未知设备';
      return `已通过服务器向 ${target} 发送完「${payload.name || '文件'}」`;
    }
    case 'file-transfer-error': {
      const target = payload.targetId ? peerLabel(payload.targetId) : '未知设备';
      return `向 ${target} 发送文件失败：${payload.message || '服务器中转错误'}`;
    }
    case 'large-file-error': {
      const target = payload.targetId ? peerLabel(payload.targetId) : '未知设备';
      return `向 ${target} 发送大文件失败：${payload.message || '传输错误'}`;
    }
    default:
      return `发送 ${type}`;
  }
};

const describeInbound = (type, payload = {}) => {
  switch (type) {
    case 'welcome':
      return `收到欢迎消息，设备 ID：${payload.id}`;
    case 'peer-joined':
      return `设备 ${payload.displayName || payload.id} 加入网络`;
    case 'peer-updated':
      return `设备 ${payload.displayName || payload.id} 信息更新`;
    case 'peer-left':
      return `设备 ${payload.displayName || payload.id} 已离开`;
    case 'registered':
      return `注册成功，当前名称为「${payload.displayName}」`;
    case 'text-message': {
      const preview = payload.message ? `：${truncate(payload.message)}` : '';
      return `${payload.displayName || payload.from} 发来消息${preview}`;
    }
    case 'clipboard-update': {
      const preview = payload.content ? `：${truncate(payload.content)}` : '';
      return `${payload.displayName || payload.from} 推送剪贴板${preview}`;
    }
    case 'signal': {
      const kind = payload.data?.type || 'signal';
      return `收到 ${kind} 信令来自 ${payload.from}`;
    }
    case 'poke':
      return `${payload.displayName || payload.from} 拍了拍你`;
    case 'file-transfer-meta':
      return `${payload.displayName || payload.from} 正在通过服务器发送「${payload.name || '文件'}」`;
    case 'file-transfer-complete':
      return `${payload.displayName || payload.from} 已通过服务器发送完「${payload.name || '文件'}」`;
    case 'file-transfer-error':
      if (payload?.targetId) {
        return `向 ${peerLabel(payload.targetId)} 发送文件失败：${payload.message || '服务器中转错误'}`;
      }
      return `${payload.displayName || payload.from || '未知设备'} 的文件发送失败：${payload.message || '服务器中转错误'}`;
    case 'large-file-meta':
      return `${payload.displayName || payload.from} 正在上传大文件「${payload.name || '文件'}」`;
    case 'large-file-progress':
      return `${payload.from || '对方'} 的大文件已上传 ${payload.percent || 0}%`;
    case 'large-file-ready':
      return `${payload.displayName || payload.from} 的大文件「${payload.name || '文件'}」已校验完成`;
    case 'large-file-error':
      if (payload?.targetId) {
        return `向 ${peerLabel(payload.targetId)} 发送大文件失败：${payload.message || '传输错误'}`;
      }
      return `${payload.displayName || payload.from || '未知设备'} 的大文件传输失败：${payload.message || '传输错误'}`;
    case 'clipboard-error':
    case 'delivery-error':
    case 'signal-error':
    case 'error':
      return payload.message ? `错误：${payload.message}` : '收到错误消息';
    case 'ping':
      return '收到服务器 ping';
    default:
      return `收到 ${type}`;
  }
};

const appendStatus = (container, message) => {
  const entry = document.createElement('p');
  const time = formatClock();
  entry.textContent = `[${time}] ${message}`;
  if (container) {
    container.appendChild(entry);
    updateStatusPanelScroll(container);
  }
  const label = container?.dataset?.activityLabel;
  appendActivity('status', label ? `${label}：${message}` : message);
};

const addMessage = ({ from, displayName, message, timestamp }) => {
  const wrapper = document.createElement('div');
  wrapper.className = 'message';

  const meta = document.createElement('div');
  meta.className = 'meta';
  const sender = from === state.selfId ? '我' : displayName || from;
  const timeText = new Date(timestamp).toLocaleTimeString('zh-CN', { hour12: false });
  meta.textContent = `${sender} • ${timeText}`;

  const body = document.createElement('p');
  body.className = 'body';
  body.textContent = message;

  wrapper.appendChild(meta);
  wrapper.appendChild(body);
  dom.messages.appendChild(wrapper);
  dom.messages.scrollTop = dom.messages.scrollHeight;
};

const persistDisplayName = (name) => {
  try {
    localStorage.setItem('snapsend:displayName', name);
  } catch {
    /* ignored */
  }
};

const loadDisplayName = () => {
  try {
    return localStorage.getItem('snapsend:displayName') || '';
  } catch {
    return '';
  }
};

const savedDisplayName = loadDisplayName();
if (savedDisplayName) {
  state.displayName = savedDisplayName;
  if (dom.displayName) {
    dom.displayName.value = savedDisplayName;
  }
  if (dom.currentDisplayName) {
    dom.currentDisplayName.textContent = savedDisplayName;
  }
}

const updateSelectors = () => {
  const selects = [dom.fileTarget, dom.clipboardTarget, dom.chatTarget];
  selects.forEach((select) => {
    const selected = select.value;
    while (select.options.length > 1) {
      select.remove(1);
    }
    for (const [peerId, peer] of state.peers) {
      const option = document.createElement('option');
      option.value = peerId;
      option.textContent = peer.displayName || peerId;
      select.appendChild(option);
    }
    if (selected && state.peers.has(selected)) {
      select.value = selected;
    } else {
      select.value = '';
    }
  });
};

const renderPeers = () => {
  dom.peerList.innerHTML = '';
  for (const [peerId, peer] of state.peers.entries()) {
    const fragment = document.importNode(dom.peerTemplate.content, true);
    fragment.querySelector('.peer-name').textContent = peer.displayName || '未命名设备';
    fragment.querySelector('.peer-id').textContent = peerId;
    const button = fragment.querySelector('.start-connection');
    button.dataset.peerId = peerId;
    button.textContent = '拍一拍';
    button.addEventListener('click', () => {
      sendWsMessage('poke', { targetId: peerId });
    });
    dom.peerList.appendChild(fragment);
  }
  updateSelectors();
};

const sendWsMessage = (type, payload) => {
  const socket = state.ws;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    appendActivity('warning', `未发送 ${type}：连接尚未建立。`);
    return;
  }
  socket.send(JSON.stringify({ type, payload }));
  if (!SILENCED_OUTBOUND_TYPES.has(type)) {
    appendActivity('outbound', describeOutbound(type, payload));
  }
};

const createPeerContext = (peerId) => {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  const ready = deferred();
  const context = {
    peerId,
    pc,
    ready,
    dataChannel: null,
    hasLocalDescription: false,
    pendingCandidates: [],
  };

  pc.onicecandidate = ({ candidate }) => {
    if (candidate) {
      sendWsMessage('signal', {
        targetId: peerId,
        data: { type: 'candidate', candidate },
      });
    }
  };

  pc.onconnectionstatechange = () => {
    const stateText = pc.connectionState;
    appendStatus(null, `与 ${peerLabel(peerId)} 的连接状态：${stateText}`);
    if (stateText === 'failed') {
      context.ready?.reject?.(new Error('数据通道连接失败。'));
    }
    if (stateText === 'failed' || stateText === 'closed' || stateText === 'disconnected') {
      cleanupPeer(peerId, { reason: 'connection-state', state: stateText });
    }
  };

  pc.ondatachannel = (event) => {
    context.dataChannel = event.channel;
    attachDataChannel(peerId, context.dataChannel, context);
  };

  state.peerConnections.set(peerId, context);
  const earlyCandidates = state.orphanCandidates.get(peerId);
  if (earlyCandidates) {
    context.pendingCandidates.push(...earlyCandidates);
    state.orphanCandidates.delete(peerId);
  }
  return context;
};

const cleanupPeer = (peerId, options = {}) => {
  const { reason = 'generic' } = options;
  const treatAsOffline = reason === 'peer-left' || reason === 'ws-closed' || reason === 'reset';
  const context = state.peerConnections.get(peerId);
  if (context) {
    try {
      if (context.dataChannel && context.dataChannel.readyState !== 'closed') {
        context.dataChannel.close();
      }
      context.pc.close();
    } catch {
      /* ignored */
    }
    state.peerConnections.delete(peerId);
  }
  for (const [key, transfer] of state.incomingTransfers.entries()) {
    if (transfer.peerId !== peerId) continue;
    if ((transfer.mode === 'relay' || transfer.mode === 'http-relay') && !treatAsOffline) {
      continue;
    }
    if (transfer.display) {
      if (transfer.mode === 'webrtc' || transfer.mode === 'direct') {
        failTransferDisplay(transfer.display, '连接已关闭。');
      } else if (transfer.mode === 'relay' || transfer.mode === 'http-relay') {
        failTransferDisplay(transfer.display, '对方已离线，服务器中转已中止。');
      }
    }
    cleanupDirectTransferStorage(transfer);
    state.incomingTransfers.delete(key);
  }
  const outgoing = state.outgoingTransfers.get(peerId);
  if (outgoing) {
    const isRelay = outgoing.mode === 'relay' || outgoing.mode === 'http-relay';
    if (!isRelay || treatAsOffline) {
      if (outgoing.display) {
        if (outgoing.mode === 'webrtc' || outgoing.mode === 'direct') {
          const filePhrase = outgoing.fileName ? `「${outgoing.fileName}」` : '文件';
          failTransferDisplay(outgoing.display, `连接已关闭，${filePhrase}可能未完成。`);
        } else if (treatAsOffline) {
          failTransferDisplay(outgoing.display, '对方已离线，服务器中转失败。');
        }
      }
      state.outgoingTransfers.delete(peerId);
    }
  }
  for (const [transferId, trackerInfo] of state.transferTrackers.entries()) {
    if (trackerInfo.peerId !== peerId) continue;
    const tracker = trackerInfo.tracker;
    if ((tracker.mode === 'relay' || tracker.mode === 'http-relay') && !treatAsOffline) {
      continue;
    }
    tracker.cancelled = true;
    tracker.cancelledReason = treatAsOffline ? '对方已离线。' : '数据通道连接失败。';
    if (tracker.display) {
      const message =
        (tracker.mode === 'relay' || tracker.mode === 'http-relay') && treatAsOffline
          ? '对方已离线，服务器中转失败。'
          : tracker.cancelledReason;
      failTransferDisplay(tracker.display, message);
    }
    state.transferTrackers.delete(transferId);
  }
  rejectDirectWaiters(peerId, createFallbackError('局域网直连已断开。'));
  state.orphanCandidates.delete(peerId);
};

const applyIceCandidate = async (context, candidateInit) => {
  try {
    await context.pc.addIceCandidate(new RTCIceCandidate(candidateInit));
  } catch (error) {
    appendStatus(
      dom.fileStatus,
      `处理 ${peerLabel(context.peerId)} 的 ICE 候选信息失败：${error.message}`,
    );
  }
};

const flushPendingCandidates = async (context) => {
  while (context.pendingCandidates.length > 0) {
    const candidateInit = context.pendingCandidates.shift();
    await applyIceCandidate(context, candidateInit);
  }
};

const attachDataChannel = (peerId, channel, context) => {
  channel.binaryType = 'arraybuffer';
  channel.bufferedAmountLowThreshold = FILE_CHUNK_SIZE * 4;

  channel.addEventListener('open', () => {
    appendStatus(dom.fileStatus, `与 ${peerLabel(peerId)} 的数据通道已建立。`);
    context.ready.resolve();
  });

  channel.addEventListener('close', () => {
    appendStatus(dom.fileStatus, `与 ${peerLabel(peerId)} 的数据通道已关闭。`);
    context.ready?.reject?.(new Error('数据通道已关闭。'));
    cleanupPeer(peerId, { reason: 'datachannel-close' });
  });

  channel.addEventListener('error', () => {
    appendStatus(dom.fileStatus, `与 ${peerLabel(peerId)} 的数据通道出现错误。`);
    context.ready?.reject?.(new Error('数据通道出现错误。'));
    cleanupPeer(peerId, { reason: 'datachannel-error' });
  });

  channel.addEventListener('message', (event) => {
    handleDataChannelMessage(peerId, event.data);
  });
};

const preparePeerConnection = async (peerId) => {
  let context = state.peerConnections.get(peerId);
  if (!context) {
    context = createPeerContext(peerId);
  }
  if (!context.dataChannel || context.dataChannel.readyState === 'closed') {
    context.dataChannel = context.pc.createDataChannel('snapsend');
    attachDataChannel(peerId, context.dataChannel, context);
  }
  if (!context.hasLocalDescription) {
    const offer = await context.pc.createOffer();
    await context.pc.setLocalDescription(offer);
    context.hasLocalDescription = true;
    sendWsMessage('signal', { targetId: peerId, data: { type: 'offer', sdp: offer } });
  }
  await context.ready.promise;
  return context;
};

const acceptOffer = async (peerId, remoteOffer) => {
  let context = state.peerConnections.get(peerId);
  if (!context) {
    context = createPeerContext(peerId);
  }
  if (context.hasLocalDescription && context.pc.currentRemoteDescription) {
    return;
  }
  await context.pc.setRemoteDescription(new RTCSessionDescription(remoteOffer));
  await flushPendingCandidates(context);
  const answer = await context.pc.createAnswer();
  await context.pc.setLocalDescription(answer);
  context.hasLocalDescription = true;
  sendWsMessage('signal', { targetId: peerId, data: { type: 'answer', sdp: answer } });
};

const acceptAnswer = async (peerId, remoteAnswer) => {
  const context = state.peerConnections.get(peerId);
  if (!context) return;
  await context.pc.setRemoteDescription(new RTCSessionDescription(remoteAnswer));
  await flushPendingCandidates(context);
};

const addIceCandidate = async (peerId, candidate) => {
  let context = state.peerConnections.get(peerId);
  if (!context) {
    const list = state.orphanCandidates.get(peerId);
    if (list) {
      list.push(candidate);
    } else {
      state.orphanCandidates.set(peerId, [candidate]);
    }
    return;
  }
  if (!context.pc.currentRemoteDescription) {
    context.pendingCandidates.push(candidate);
    return;
  }
  await applyIceCandidate(context, candidate);
};

const getPeerChannel = (peerId) => state.peerConnections.get(peerId)?.dataChannel || null;

const sendDirectMessageToPeer = (peerId, message) => {
  const channel = getPeerChannel(peerId);
  if (!channel || channel.readyState !== 'open') return false;
  channel.send(JSON.stringify(message));
  return true;
};

const failIncomingDirectTransfer = async (peerId, transfer, message) => {
  if (transfer?.display) {
    failTransferDisplay(transfer.display, message);
  }
  if (transfer) {
    await cleanupDirectTransferStorage(transfer);
    state.incomingTransfers.delete(directKey(transfer.meta.id));
  }
  sendDirectMessageToPeer(peerId, {
    type: 'direct-file-error',
    id: transfer?.meta?.id,
    message,
  });
};

const handleDirectFileOffer = async (peerId, message) => {
  const { id, name, size, mime, chunkSize } = message;
  if (!id || !Number.isFinite(size) || size < 0) {
    sendDirectMessageToPeer(peerId, {
      type: 'direct-file-reject',
      id,
      message: '直连文件元数据无效。',
    });
    return;
  }

  const storage = await createDirectReceiveStorage({
    transferId: id,
    peerId,
    name,
    size,
    mime,
  });
  if (storage?.rejected) {
    sendDirectMessageToPeer(peerId, {
      type: 'direct-file-reject',
      id,
      message: storage.message,
    });
    return;
  }

  const display = createTransferDisplay({
    direction: 'inbound',
    peerId,
    name,
    size,
  });
  const hasher = await createSha256();
  const chunkHasher = await createSha256();
  const transfer = {
    meta: {
      id,
      name,
      size,
      mime,
      chunkSize,
    },
    receivedBytes: 0,
    nextIndex: 0,
    pendingChunk: null,
    display,
    peerId,
    mode: 'direct',
    hasher,
    chunkHasher,
    ...storage,
  };
  state.incomingTransfers.set(directKey(id), transfer);
  updateTransferDisplay(display, {
    percent: 0,
    status:
      storage.storage === 'direct-save'
        ? '局域网直连已建立，正在实时保存到本地文件…'
        : storage.storage === 'opfs'
        ? '局域网直连已建立，正在写入浏览器临时文件…'
        : '局域网直连已建立，正在内存缓冲接收…',
  });
  appendStatus(
    dom.fileStatus,
    `准备通过局域网直连接收 ${peerLabel(peerId)} 的 "${name || '文件'}"（${humanFileSize(size)}）。`,
  );
  sendDirectMessageToPeer(peerId, {
    type: 'direct-file-accept',
    id,
    storage: storage.storage,
  });
};

const handleDirectFileChunkMeta = async (peerId, message) => {
  const transfer = state.incomingTransfers.get(directKey(message.id));
  if (!transfer || transfer.peerId !== peerId || transfer.mode !== 'direct') {
    sendDirectMessageToPeer(peerId, {
      type: 'direct-file-error',
      id: message.id,
      message: '接收端没有找到对应的直连传输。',
    });
    return;
  }

  const { index, offset, size, sha256 } = message;
  if (
    index !== transfer.nextIndex ||
    offset !== transfer.receivedBytes ||
    !Number.isFinite(size) ||
    size <= 0 ||
    !sha256
  ) {
    await failIncomingDirectTransfer(peerId, transfer, '直连分片顺序或校验信息异常。');
    return;
  }

  transfer.pendingChunk = {
    index,
    offset,
    size,
    sha256,
  };
};

const handleDirectBinaryChunk = async (peerId, data) => {
  const transfer = Array.from(state.incomingTransfers.values()).find(
    (item) => item.peerId === peerId && item.mode === 'direct' && item.pendingChunk,
  );
  if (!transfer) return;

  const chunkMeta = transfer.pendingChunk;
  transfer.pendingChunk = null;
  const bytes = new Uint8Array(data);
  try {
    if (bytes.byteLength !== chunkMeta.size) {
      throw new Error('直连分片大小不一致。');
    }
    const chunkHash = await hashBytes(transfer.chunkHasher, bytes);
    if (chunkHash !== chunkMeta.sha256) {
      throw new Error('直连分片 SHA-256 校验失败。');
    }

    if (transfer.storage === 'opfs' || transfer.storage === 'direct-save') {
      await transfer.writable.write(bytes);
    } else {
      transfer.chunks.push(data);
    }
    transfer.hasher.update(bytes);
    transfer.receivedBytes += bytes.byteLength;
    transfer.nextIndex += 1;
    const percent = transfer.meta.size
      ? Math.min(100, Math.round((transfer.receivedBytes / transfer.meta.size) * 100))
      : 100;
    updateTransferDisplay(transfer.display, {
      percent,
      status: `局域网直连接收 ${percent}% · 已校验分片 ${transfer.nextIndex}`,
    });
    sendDirectMessageToPeer(peerId, {
      type: 'direct-file-chunk-ack',
      id: transfer.meta.id,
      index: chunkMeta.index,
      receivedBytes: transfer.receivedBytes,
      percent,
    });
  } catch (error) {
    await failIncomingDirectTransfer(peerId, transfer, error.message);
  }
};

const handleDirectFileComplete = async (peerId, message) => {
  const transfer = state.incomingTransfers.get(directKey(message.id));
  if (!transfer || transfer.peerId !== peerId || transfer.mode !== 'direct') {
    sendDirectMessageToPeer(peerId, {
      type: 'direct-file-error',
      id: message.id,
      message: '接收端没有找到对应的直连传输。',
    });
    return;
  }

  try {
    if (transfer.pendingChunk) {
      throw new Error('还有直连分片未写入。');
    }
    if (message.totalChunks !== transfer.nextIndex) {
      throw new Error('直连分片数量不一致。');
    }
    if (transfer.receivedBytes !== transfer.meta.size) {
      throw new Error('直连接收字节数不一致。');
    }
    const digest = transfer.hasher.digest('hex');
    if (digest !== message.sha256) {
      throw new Error('直连整文件 SHA-256 校验失败。');
    }
    await finalizeDirectDownload(transfer, digest);
    state.incomingTransfers.delete(directKey(message.id));
    sendDirectMessageToPeer(peerId, {
      type: 'direct-file-complete-ack',
      id: message.id,
      sha256: digest,
    });
  } catch (error) {
    await failIncomingDirectTransfer(peerId, transfer, error.message);
  }
};

const handleDirectRemoteError = (peerId, message) => {
  const error = createFallbackError(message.message || '局域网直连传输失败。');
  rejectDirectWaitersForTransfer(message.id, error);
  const trackerInfo = state.transferTrackers.get(message.id);
  if (trackerInfo) {
    trackerInfo.tracker.cancelled = true;
    trackerInfo.tracker.cancelledReason = error.message;
    state.transferTrackers.delete(message.id);
  }
  const transfer = state.incomingTransfers.get(directKey(message.id));
  if (transfer) {
    if (transfer.display) {
      failTransferDisplay(transfer.display, error.message);
    }
    cleanupDirectTransferStorage(transfer);
    state.incomingTransfers.delete(directKey(message.id));
  }
  appendStatus(dom.fileStatus, `${peerLabel(peerId)} 的局域网直连失败：${error.message}`);
};

const handleLegacyDataChannelMessage = (peerId, message) => {
  if (message.type === 'file-meta') {
    const key = dataChannelKey(peerId);
    const display = createTransferDisplay({
      direction: 'inbound',
      peerId,
      name: message.name,
      size: message.size,
    });
    state.incomingTransfers.set(key, {
      meta: message,
      chunks: [],
      receivedBytes: 0,
      display,
      peerId,
      mode: 'webrtc',
    });
    updateTransferDisplay(display, {
      percent: 0,
      status: `等待来自 ${peerLabel(peerId)} 的数据…`,
    });
    appendStatus(
      dom.fileStatus,
      `正在接收来自 ${peerLabel(peerId)} 的 "${message.name}"（${humanFileSize(message.size)}）`,
    );
  } else if (message.type === 'file-complete') {
    const key = dataChannelKey(peerId);
    const transfer = state.incomingTransfers.get(key);
    if (!transfer) return;
    if (message.id && transfer.meta.id && message.id !== transfer.meta.id) {
      return;
    }
    const blob = new Blob(transfer.chunks, { type: transfer.meta.mime || 'application/octet-stream' });
    const filename = transfer.meta.name || '接收文件';
    const downloadUrl = triggerDownload(blob, filename);
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.download = filename;
    link.textContent = `重新下载 ${filename}`;
    link.className = 'download-link';
    link.rel = 'noopener';
    const actions = document.createElement('div');
    actions.className = 'transfer-actions';
    actions.appendChild(link);
    if (transfer.display) {
      completeTransferDisplay(transfer.display, `已接收完成，正在保存 "${filename}"`);
      transfer.display.entry.appendChild(actions);
      updateStatusPanelScroll(dom.fileStatus);
    } else if (dom.fileStatus) {
      dom.fileStatus.appendChild(actions);
      updateStatusPanelScroll(dom.fileStatus);
    }
    appendStatus(dom.fileStatus, `文件 "${transfer.meta.name}" 已保存并可再次下载。`);
    state.incomingTransfers.delete(key);
    setTimeout(() => {
      URL.revokeObjectURL(downloadUrl);
      if (link.isConnected) {
        link.textContent = `${filename} 下载链接已过期`;
        link.removeAttribute('href');
        link.classList.add('download-link-disabled');
      }
    }, 5 * 60 * 1000);
  }
};

const handleDataChannelMessage = (peerId, data) => {
  if (typeof data === 'string') {
    try {
      const message = JSON.parse(data);
      if (message.type === 'direct-file-offer') {
        handleDirectFileOffer(peerId, message).catch((error) => {
          appendStatus(dom.fileStatus, `处理直连请求失败：${error.message}`);
        });
      } else if (message.type === 'direct-file-accept' || message.type === 'direct-file-reject') {
        settleDirectControl(message);
      } else if (message.type === 'direct-file-chunk') {
        handleDirectFileChunkMeta(peerId, message).catch((error) => {
          appendStatus(dom.fileStatus, `处理直连分片信息失败：${error.message}`);
        });
      } else if (message.type === 'direct-file-chunk-ack') {
        settleDirectChunkAck(message);
      } else if (message.type === 'direct-file-complete') {
        handleDirectFileComplete(peerId, message).catch((error) => {
          appendStatus(dom.fileStatus, `完成直连传输失败：${error.message}`);
        });
      } else if (message.type === 'direct-file-complete-ack') {
        settleDirectControl(message);
      } else if (message.type === 'direct-file-error') {
        handleDirectRemoteError(peerId, message);
      } else {
        handleLegacyDataChannelMessage(peerId, message);
      }
    } catch {
      /* ignored */
    }
  } else if (data instanceof ArrayBuffer) {
    const hasDirectPendingChunk = Array.from(state.incomingTransfers.values()).some(
      (item) => item.peerId === peerId && item.mode === 'direct' && item.pendingChunk,
    );
    if (hasDirectPendingChunk) {
      handleDirectBinaryChunk(peerId, data).catch((error) => {
        appendStatus(dom.fileStatus, `写入直连分片失败：${error.message}`);
      });
      return;
    }
    const key = dataChannelKey(peerId);
    const transfer = state.incomingTransfers.get(key);
    if (!transfer) return;
    transfer.chunks.push(data);
    transfer.receivedBytes += data.byteLength;
    const totalSize = transfer.meta.size || 0;
    const percent = totalSize
      ? Math.min(100, Math.round((transfer.receivedBytes / totalSize) * 100))
      : 0;
    if (transfer.display) {
      updateTransferDisplay(transfer.display, {
        percent,
        status: `已接收 ${percent}%`,
      });
    }
  }
};

const waitForSendBuffer = (channel) =>
  new Promise((resolve) => {
    if (channel.bufferedAmount < channel.bufferedAmountLowThreshold) {
      resolve();
      return;
    }
    const handler = () => {
      channel.removeEventListener('bufferedamountlow', handler);
      resolve();
    };
    channel.addEventListener('bufferedamountlow', handler);
  });

const createServerTransfer = async (peerId, file) => {
  const response = await fetch('/api/transfers', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      senderId: state.selfId,
      targetId: peerId,
      name: file.name,
      size: file.size,
      mime: file.type || 'application/octet-stream',
      chunkSize: HTTP_FILE_CHUNK_SIZE,
    }),
  });
  return readJsonResponse(response);
};

const uploadTransferChunk = async ({
  transferId,
  uploadToken,
  index,
  offset,
  chunk,
  chunkHash,
}) => {
  const response = await fetch(
    `/api/transfers/${encodeURIComponent(transferId)}/chunks/${index}`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Upload-Token': uploadToken,
        'X-Chunk-Offset': String(offset),
        'X-Chunk-Size': String(chunk.byteLength),
        'X-Chunk-SHA256': chunkHash,
      },
      body: chunk,
    },
  );
  return readJsonResponse(response);
};

const completeServerTransfer = async ({ transferId, uploadToken, sha256, totalChunks }) => {
  const response = await fetch(`/api/transfers/${encodeURIComponent(transferId)}/complete`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      uploadToken,
      sha256,
      totalChunks,
    }),
  });
  return readJsonResponse(response);
};

const cancelServerTransfer = async (transferId, uploadToken) => {
  if (!transferId || !uploadToken) return;
  await fetch(`/api/transfers/${encodeURIComponent(transferId)}`, {
    method: 'DELETE',
    headers: {
      'X-Upload-Token': uploadToken,
    },
  }).catch(() => {});
};

const sendFileViaDirect = async (peerId, file, display, tracker) => {
  if (!('RTCPeerConnection' in window)) {
    throw createFallbackError('当前浏览器不支持局域网直连。');
  }

  updateTransferDisplay(display, {
    percent: 0,
    status: '正在尝试局域网直连…',
  });

  const context = await withTimeout(
    preparePeerConnection(peerId),
    DIRECT_CONNECT_TIMEOUT,
    createFallbackError('局域网直连握手超时。'),
  );
  const channel = context.dataChannel;
  if (!channel || channel.readyState !== 'open') {
    throw createFallbackError('局域网直连通道不可用。');
  }

  const transferId = `direct-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  tracker.mode = 'direct';
  tracker.transferId = transferId;
  tracker.cancelled = false;
  tracker.cancelledReason = undefined;
  state.outgoingTransfers.set(peerId, tracker);
  state.transferTrackers.set(transferId, { tracker, peerId });

  const offerResponsePromise = waitForDirectControl(
    transferId,
    ['direct-file-accept', 'direct-file-reject'],
    peerId,
  );
  sendDataChannelMessage(channel, {
    type: 'direct-file-offer',
    id: transferId,
    name: file.name,
    size: file.size,
    mime: file.type || 'application/octet-stream',
    chunkSize: DIRECT_FILE_CHUNK_SIZE,
  });

  const response = await offerResponsePromise;
  if (response.type === 'direct-file-reject') {
    throw createFallbackError(response.message || '接收端拒绝局域网直连。');
  }

  updateTransferDisplay(display, {
    percent: 0,
    status:
      response.storage === 'direct-save'
        ? '局域网直连已建立，正在实时写入接收端文件…'
        : response.storage === 'opfs'
        ? '局域网直连已建立，正在向接收端临时文件写入…'
        : '局域网直连已建立，正在发送…',
  });
  appendStatus(dom.fileStatus, `已与 ${peerLabel(peerId)} 建立局域网直连，开始发送 "${file.name}"。`);

  const fileHasher = await createSha256();
  const chunkHasher = await createSha256();
  let offset = 0;
  let chunkIndex = 0;
  const totalChunks = file.size ? Math.ceil(file.size / DIRECT_FILE_CHUNK_SIZE) : 0;

  while (offset < file.size) {
    const slice = file.slice(offset, offset + DIRECT_FILE_CHUNK_SIZE);
    // eslint-disable-next-line no-await-in-loop
    const buffer = await slice.arrayBuffer();
    const chunk = new Uint8Array(buffer);
    fileHasher.update(chunk);
    // eslint-disable-next-line no-await-in-loop
    const chunkHash = await hashBytes(chunkHasher, chunk);
    const ackPromise = waitForDirectChunkAck(transferId, chunkIndex, peerId);
    sendDataChannelMessage(channel, {
      type: 'direct-file-chunk',
      id: transferId,
      index: chunkIndex,
      offset,
      size: chunk.byteLength,
      sha256: chunkHash,
    });
    channel.send(buffer);
    // eslint-disable-next-line no-await-in-loop
    await waitForSendBuffer(channel);
    // eslint-disable-next-line no-await-in-loop
    await ackPromise;

    offset += chunk.byteLength;
    chunkIndex += 1;
    const percent = file.size ? Math.min(100, Math.round((offset / file.size) * 100)) : 100;
    updateTransferDisplay(display, {
      percent,
      status: `局域网直连发送 ${percent}% · 已确认分片 ${chunkIndex}/${totalChunks}`,
    });
    if (tracker.cancelled) {
      throw createFallbackError(tracker.cancelledReason || '局域网直连已中止。');
    }
    if (chunkIndex % 4 === 0) {
      // eslint-disable-next-line no-await-in-loop
      await microDelay();
    }
  }

  const fileHash = fileHasher.digest('hex');
  const completeAckPromise = waitForDirectControl(
    transferId,
    'direct-file-complete-ack',
    peerId,
    DIRECT_CHUNK_ACK_TIMEOUT,
  );
  sendDataChannelMessage(channel, {
    type: 'direct-file-complete',
    id: transferId,
    sha256: fileHash,
    totalChunks,
  });
  const completeAck = await completeAckPromise;
  completeTransferDisplay(
    display,
    `已通过局域网直连发送，SHA-256：${shortHash(completeAck.sha256 || fileHash)}`,
  );
  appendStatus(dom.fileStatus, `局域网直连发送完成：${file.name}。`);
  state.transferTrackers.delete(transferId);
};

const sendFileViaHttpRelay = async (peerId, file, display, tracker) => {
  const socket = state.ws;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    const message = '无法连接服务器进行文件中转。';
    appendStatus(dom.fileStatus, message);
    if (display) {
      failTransferDisplay(display, message);
    }
    return;
  }
  if (tracker.transferId) {
    state.transferTrackers.delete(tracker.transferId);
  }
  updateTransferDisplay(display, {
    percent: 0,
    status: '正在创建安全传输会话…',
  });

  const session = await createServerTransfer(peerId, file);
  const transferId = session.transferId;
  const uploadToken = session.uploadToken;
  const chunkSize = session.chunkSize || HTTP_FILE_CHUNK_SIZE;
  tracker.mode = 'http-relay';
  tracker.transferId = transferId;
  tracker.uploadToken = uploadToken;
  tracker.cancelled = false;
  tracker.cancelledReason = undefined;
  state.outgoingTransfers.set(peerId, tracker);
  state.transferTrackers.set(transferId, { tracker, peerId });
  updateTransferDisplay(display, {
    percent: 0,
    status: `正在分片上传「${file.name || '文件'}」…`,
  });
  appendStatus(dom.fileStatus, `开始向 ${peerLabel(peerId)} 上传 "${file.name}"，分片大小 ${humanFileSize(chunkSize)}。`);

  const fileHasher = await createSha256();
  const chunkHasher = await createSha256();
  let offset = 0;
  let chunkIndex = 0;
  const totalChunks = file.size ? Math.ceil(file.size / chunkSize) : 0;

  while (offset < file.size) {
    const slice = file.slice(offset, offset + chunkSize);
    // eslint-disable-next-line no-await-in-loop
    const buffer = await slice.arrayBuffer();
    const chunk = new Uint8Array(buffer);
    fileHasher.update(chunk);
    // eslint-disable-next-line no-await-in-loop
    const chunkHash = await hashBytes(chunkHasher, chunk);
    // eslint-disable-next-line no-await-in-loop
    await uploadTransferChunk({
      transferId,
      uploadToken,
      index: chunkIndex,
      offset,
      chunk,
      chunkHash,
    });

    offset += chunk.byteLength;
    chunkIndex += 1;
    const percent = file.size ? Math.min(100, Math.round((offset / file.size) * 100)) : 100;
    updateTransferDisplay(display, {
      percent,
      status: `已上传 ${percent}% · 已校验分片 ${chunkIndex}/${totalChunks}`,
    });

    if (tracker.cancelled) {
      throw new Error(tracker.cancelledReason || '对方已取消接收。');
    }
    if (chunkIndex % 2 === 0) {
      // eslint-disable-next-line no-await-in-loop
      await microDelay();
    }
  }

  const fileHash = fileHasher.digest('hex');
  updateTransferDisplay(display, {
    percent: 100,
    status: '上传完成，服务器正在校验整文件…',
  });
  const completion = await completeServerTransfer({
    transferId,
    uploadToken,
    sha256: fileHash,
    totalChunks,
  });
  appendStatus(
    dom.fileStatus,
    `服务器已校验 "${file.name}"，SHA-256：${shortHash(completion.sha256 || fileHash)}。`,
  );
  completeTransferDisplay(display, `已完成并校验 SHA-256：${shortHash(completion.sha256 || fileHash)}`);
  state.transferTrackers.delete(transferId);
};

const sendFile = async (peerId, file) => {
  const display = createTransferDisplay({
    direction: 'outbound',
    peerId,
    name: file.name,
    size: file.size,
  });
  if (display) {
    updateTransferDisplay(display, { percent: 0, status: '准备局域网直连…' });
  }
  const tracker = { display, fileName: file.name, mode: 'direct', cancelled: false, transferId: undefined };
  state.outgoingTransfers.set(peerId, tracker);

  try {
    try {
      await sendFileViaDirect(peerId, file, display, tracker);
    } catch (directError) {
      const directReason =
        tracker.cancelled === true
          ? tracker.cancelledReason || directError.message
          : directError.message;
      if (tracker.transferId) {
        const channel = getPeerChannel(peerId);
        if (channel?.readyState === 'open') {
          channel.send(JSON.stringify({
            type: 'direct-file-error',
            id: tracker.transferId,
            message: '发送端切换到服务器中转。',
          }));
        }
        rejectDirectWaitersForTransfer(tracker.transferId, createFallbackError(directReason));
        state.transferTrackers.delete(tracker.transferId);
        tracker.transferId = undefined;
      }
      tracker.cancelled = false;
      tracker.cancelledReason = undefined;
      appendStatus(dom.fileStatus, `局域网直连不可用，切换到服务器中转：${directReason}`);
      updateTransferDisplay(display, {
        percent: 0,
        status: '局域网直连不可用，正在切换到服务器中转…',
      });
      await sendFileViaHttpRelay(peerId, file, display, tracker);
    }
  } catch (error) {
    const reason =
      tracker.cancelled === true
        ? tracker.cancelledReason || error.message
        : error.message;
    await cancelServerTransfer(tracker.transferId, tracker.uploadToken);
    if (tracker.transferId) {
      state.transferTrackers.delete(tracker.transferId);
      tracker.transferId = undefined;
    }
    if (display) {
      failTransferDisplay(display, `服务器中转失败：${reason}`);
    }
    appendStatus(dom.fileStatus, `服务器中转发送失败：${reason || '请稍后重试。'}`);
    tracker.cancelled = false;
    tracker.cancelledReason = undefined;
  } finally {
    if (tracker.transferId) {
      state.transferTrackers.delete(tracker.transferId);
      tracker.transferId = undefined;
    }
    state.outgoingTransfers.delete(peerId);
  }
};

const requestClipboardWrite = async (
  descriptor,
  { successMessage, failureMessage } = {},
) => {
  const success =
    typeof successMessage === 'string'
      ? successMessage
      : '已将收到的内容写入本地剪贴板';
  const failure =
    typeof failureMessage === 'string'
      ? failureMessage
      : '写入剪贴板失败';
  try {
    const result = await writeClipboardData(descriptor);
    let note = '';
    if (result.mode === 'rich' && !result.fallback) {
      note = '（保留原始格式）';
    } else if (result.mode === 'text' && result.fallback) {
      note = '（浏览器仅允许纯文本）';
    }
    appendStatus(dom.clipboardStatus, `${success}${note}。`);
    return result;
  } catch (error) {
    appendStatus(dom.clipboardStatus, `${failure}：${error.message}`);
    return null;
  }
};

const sendClipboardContent = (descriptor, targetId) => {
  const normalized = normalizeClipboardDescriptor(descriptor);
  if (!hasClipboardData(normalized)) {
    appendStatus(dom.clipboardStatus, '同步前请先提供剪贴板内容。');
    return;
  }
  const payload = { ...normalized };
  if (targetId) {
    payload.targetId = targetId;
  }
  sendWsMessage('clipboard-update', payload);
  const hint = clipboardFormatHint(payload);
  appendStatus(dom.clipboardStatus, `已发送剪贴板内容${hint}。`);
  state.lastClipboardPayload = payload;
  state.clipboardTextLinkedPayload = payload;
};

const copyDescriptorWithExecCommand = ({ text, html }) => {
  const selection = document.getSelection();
  const previousRanges = [];
  if (selection && selection.rangeCount) {
    for (let i = 0; i < selection.rangeCount; i += 1) {
      previousRanges.push(selection.getRangeAt(i));
    }
  }

  const container = document.createElement('div');
  container.contentEditable = 'true';
  container.style.position = 'fixed';
  container.style.pointerEvents = 'none';
  container.style.opacity = '0';
  container.style.bottom = '0';
  container.style.right = '0';
  container.style.whiteSpace = 'pre-wrap';
  if (html) {
    container.innerHTML = html;
  } else {
    container.textContent = text || '';
  }
  document.body.appendChild(container);

  const range = document.createRange();
  range.selectNodeContents(container);

  if (selection) {
    selection.removeAllRanges();
    selection.addRange(range);
  }

  let success = false;
  try {
    success = document.execCommand('copy');
  } catch {
    success = false;
  }

  if (selection) {
    selection.removeAllRanges();
    previousRanges.forEach((r) => selection.addRange(r));
  }

  container.remove();
  return success;
};

dom.saveDisplayName.addEventListener('click', () => {
  const name = dom.displayName.value.trim();
  if (!name) return;
  state.displayName = name;
  dom.currentDisplayName.textContent = name;
  persistDisplayName(name);
  sendWsMessage('register', { displayName: name });
  state.hasSentInitialRegister = true;
});

dom.sendFileBtn.addEventListener('click', () => {
  const peerId = dom.fileTarget.value;
  const file = dom.fileInput.files?.[0];
  if (!peerId) {
    appendStatus(dom.fileStatus, '请选择要发送的目标设备。');
    return;
  }
  if (!file) {
    appendStatus(dom.fileStatus, '请选择要发送的文件。');
    return;
  }
  sendFile(peerId, file)
    .then(() => {
      if (dom.fileInput) {
        dom.fileInput.value = '';
      }
    })
    .catch((error) => {
      appendStatus(dom.fileStatus, `文件传输失败：${error.message}`);
    });
});

dom.pushClipboardBtn.addEventListener('click', async () => {
  const manualText = dom.clipboardText.value;
  if (manualText) {
    const descriptor = normalizeClipboardDescriptor({ content: manualText });
    state.clipboardTextLinkedPayload = descriptor;
    sendClipboardContent(descriptor, dom.clipboardTarget.value);
    return;
  }

  const manualFallback = async () => {
    const descriptor = await openClipboardOverlay({
      confirmLabel: '推送此剪贴板',
      message: '请在下方区域按 Ctrl+V / ⌘V 粘贴要同步的内容，我们会尽可能保留格式。',
    });
    if (!descriptor) {
      return false;
    }
    if (!hasClipboardData(descriptor)) {
      appendStatus(dom.clipboardStatus, '剪贴板为空，无内容可同步。');
      return false;
    }
    dom.clipboardText.value = descriptor.content || '';
    state.clipboardTextLinkedPayload = descriptor;
    sendClipboardContent(descriptor, dom.clipboardTarget.value);
    return true;
  };

  try {
    const { descriptor, mode } = await readSystemClipboardDescriptor();
    if (!descriptor || !hasClipboardData(descriptor)) {
      await manualFallback();
      return;
    }
    dom.clipboardText.value = descriptor.content || '';
    state.clipboardTextLinkedPayload = descriptor;
    const hint = clipboardFormatHint(descriptor);
    if ((mode === 'rich' || mode === 'command' || mode === 'manual') && hint) {
      appendStatus(dom.clipboardStatus, `已读取系统剪贴板${hint}。`);
    }
    sendClipboardContent(descriptor, dom.clipboardTarget.value);
  } catch (error) {
    appendStatus(
      dom.clipboardStatus,
      `读取剪贴板失败：${error.message || '请授予权限或手动粘贴内容。'}`,
    );
    await manualFallback();
  }
});

dom.readAndBroadcastClipboardBtn.addEventListener('click', async () => {
  const manualFallback = async () => {
    const descriptor = await openClipboardOverlay({
      confirmLabel: '广播此内容',
      message: '无法直接读取剪贴板，请在下方区域按 Ctrl+V / ⌘V 粘贴需要广播的内容，我们会保留格式。',
    });
    if (!descriptor) {
      return false;
    }
    if (!hasClipboardData(descriptor)) {
      appendStatus(dom.clipboardStatus, '剪贴板为空，无内容可广播。');
      return false;
    }
    dom.clipboardText.value = descriptor.content || '';
    state.clipboardTextLinkedPayload = descriptor;
    sendClipboardContent(descriptor, dom.clipboardTarget.value);
    return true;
  };

  try {
    const { descriptor } = await readSystemClipboardDescriptor();
    if (!descriptor || !hasClipboardData(descriptor)) {
      await manualFallback();
      return;
    }
    dom.clipboardText.value = descriptor.content || '';
    state.clipboardTextLinkedPayload = descriptor;
    sendClipboardContent(descriptor, dom.clipboardTarget.value);
  } catch (error) {
    appendStatus(
      dom.clipboardStatus,
      `读取剪贴板失败：${error.message || '请授权后重试。'}`,
    );
    await manualFallback();
  }
});

dom.copyClipboardBtn.addEventListener('click', async () => {
  const linkedPayload = hasClipboardData(state.clipboardTextLinkedPayload)
    ? state.clipboardTextLinkedPayload
    : null;
  const manualText = dom.clipboardText.value;
  const descriptor = linkedPayload || (manualText ? { content: manualText } : null);

  if (!descriptor) {
    appendStatus(dom.clipboardStatus, '复制前请先提供剪贴板内容。');
    return;
  }

  const text = extractPlainText(descriptor, manualText);
  const html = descriptorHasHtml(descriptor) ? getDescriptorHtml(descriptor) : '';
  const prefersRichCopy = Boolean(html);

  let copied = false;
  let writeError = null;

  if (navigator.clipboard) {
    try {
      const result = await writeClipboardData(descriptor, {
        allowTextFallback: !prefersRichCopy,
      });
      let note = '';
      if (result.mode === 'rich' && !result.fallback) {
        note = '（保留原始格式）';
      } else if (result.mode === 'text' && result.fallback) {
        note = '（浏览器仅允许纯文本）';
      }
      appendStatus(dom.clipboardStatus, `已将内容复制到本地剪贴板${note}。`);
      copied = true;
    } catch (error) {
      writeError = error;
    }
  }

  if (copied) {
    return;
  }

  const fallbackSuccess = copyDescriptorWithExecCommand({ text, html });
  if (fallbackSuccess) {
    appendStatus(dom.clipboardStatus, '已通过浏览器复制命令复制剪贴板内容。');
    return;
  }

  const reason = writeError?.message || '浏览器拒绝复制操作。';
  appendStatus(dom.clipboardStatus, `复制失败，请手动选择文本并复制：${reason}`);
});

statusPanels.forEach((panel) => {
  panel.addEventListener('toggle', () => {
    if (panel.open) {
      panel.classList.remove('has-updates');
      const log = panel.querySelector('.status-log');
      if (log) {
        log.scrollTop = log.scrollHeight;
      }
    }
  });
});

dom.clipboardText.addEventListener('input', () => {
  state.clipboardTextLinkedPayload = null;
});

dom.chatForm.addEventListener('submit', (event) => {
  event.preventDefault();
  const message = dom.chatInput.value.trim();
  if (!message) return;
  const payload = { message };
  if (dom.chatTarget.value) {
    payload.targetId = dom.chatTarget.value;
  }
  sendWsMessage('text-message', payload);
  addMessage({
    from: state.selfId,
    displayName: state.displayName,
    message,
    timestamp: Date.now(),
  });
  dom.chatInput.value = '';
  dom.chatInput.focus();
});

const handleSocketMessage = async (event) => {
  let data;
  try {
    data = JSON.parse(event.data);
  } catch {
    return;
  }
  const { type, payload } = data;
  if (!SILENCED_INBOUND_TYPES.has(type)) {
    const kind = typeof type === 'string' && type.endsWith('error') ? 'error' : type === 'error' ? 'error' : 'inbound';
    appendActivity(kind, describeInbound(type, payload));
  }
  switch (type) {
    case 'welcome': {
      state.selfId = payload.id;
      dom.selfId.textContent = payload.id;
      if (!state.displayName && payload.displayName) {
        state.displayName = payload.displayName;
        dom.currentDisplayName.textContent = payload.displayName;
        dom.displayName.value = payload.displayName;
        persistDisplayName(payload.displayName);
      }
      if (
        state.displayName &&
        payload.displayName &&
        payload.displayName !== state.displayName &&
        !state.hasSentInitialRegister
      ) {
        sendWsMessage('register', { displayName: state.displayName });
        state.hasSentInitialRegister = true;
      }
      if (payload.peers) {
        payload.peers.forEach((peer) => {
          state.peers.set(peer.id, peer);
        });
        renderPeers();
      }
      setServerStatus(`已注册 · ${formatClock()}`);
      break;
    }
    case 'peer-joined':
    case 'peer-updated': {
      state.peers.set(payload.id, payload);
      renderPeers();
      break;
    }
    case 'peer-left': {
      state.peers.delete(payload.id);
      cleanupPeer(payload.id, { reason: 'peer-left' });
      renderPeers();
      break;
    }
    case 'register': {
      break;
    }
    case 'registered': {
      if (payload.displayName) {
        state.displayName = payload.displayName;
        if (dom.displayName) {
          dom.displayName.value = payload.displayName;
        }
        if (dom.currentDisplayName) {
          dom.currentDisplayName.textContent = payload.displayName;
        }
        persistDisplayName(payload.displayName);
        state.hasSentInitialRegister = true;
      }
      break;
    }
    case 'signal': {
      const { from, data: signalData } = payload;
      if (!signalData) break;
      if (signalData.type === 'offer') {
        await acceptOffer(from, signalData.sdp);
      } else if (signalData.type === 'answer') {
        await acceptAnswer(from, signalData.sdp);
      } else if (signalData.type === 'candidate') {
        await addIceCandidate(from, signalData.candidate);
      }
      break;
    }
    case 'text-message': {
      addMessage(payload);
      break;
    }
    case 'clipboard-update': {
      const descriptor = normalizeClipboardDescriptor(payload);
      const textContent = extractPlainText(descriptor, '');
      if (typeof descriptor.content !== 'string') {
        descriptor.content = textContent;
      }
      dom.clipboardText.value = textContent;
      state.lastClipboardPayload = descriptor;
      state.clipboardTextLinkedPayload = descriptor;
      const hint = clipboardFormatHint(descriptor);
      appendStatus(
        dom.clipboardStatus,
        `收到 ${payload.displayName || payload.from} 的剪贴板内容${hint}。`,
      );
      if (navigator.clipboard) {
        requestClipboardWrite(descriptor);
      }
      break;
    }
    case 'poke': {
      break;
    }
    case 'large-file-meta': {
      const { from, transferId, name, size, mime } = payload;
      if (!from || !transferId) {
        break;
      }
      const key = relayKey(transferId);
      const display = createTransferDisplay({
        direction: 'inbound',
        peerId: from,
        name,
        size,
      });
      state.incomingTransfers.set(key, {
        meta: { id: transferId, name, size, mime },
        receivedBytes: 0,
        display,
        peerId: from,
        mode: 'http-relay',
      });
      updateTransferDisplay(display, {
        percent: 0,
        status: `等待 ${peerLabel(from)} 上传到本机服务器…`,
      });
      appendStatus(
        dom.fileStatus,
        `${peerLabel(from)} 正在发送 "${name || '文件'}"（${humanFileSize(size)}），服务器将先落盘校验。`,
      );
      break;
    }
    case 'large-file-progress': {
      const { transferId, receivedBytes, size, percent } = payload;
      if (!transferId) {
        break;
      }
      const key = relayKey(transferId);
      const transfer = state.incomingTransfers.get(key);
      if (!transfer) {
        break;
      }
      transfer.receivedBytes = receivedBytes || transfer.receivedBytes || 0;
      const resolvedPercent =
        typeof percent === 'number'
          ? percent
          : size
          ? Math.min(100, Math.round((transfer.receivedBytes / size) * 100))
          : 0;
      updateTransferDisplay(transfer.display, {
        percent: resolvedPercent,
        status: `发送方上传并校验分片 ${resolvedPercent}%`,
      });
      break;
    }
    case 'large-file-ready': {
      const { from, transferId, name, size, mime, sha256, downloadUrl } = payload;
      if (!transferId || !downloadUrl) {
        break;
      }
      const key = relayKey(transferId);
      let transfer = state.incomingTransfers.get(key);
      if (!transfer) {
        const display = createTransferDisplay({
          direction: 'inbound',
          peerId: from,
          name,
          size,
        });
        transfer = {
          meta: { id: transferId, name, size, mime },
          receivedBytes: size || 0,
          display,
          peerId: from,
          mode: 'http-relay',
        };
        state.incomingTransfers.set(key, transfer);
      }
      transfer.meta.sha256 = sha256;
      transfer.meta.downloadUrl = downloadUrl;
      if (transfer.display?.meta) {
        transfer.display.meta.textContent = `${humanFileSize(size)} · SHA-256 ${shortHash(sha256)}`;
      }
      completeTransferDisplay(
        transfer.display,
        `服务器校验完成，可下载 "${name || transfer.meta.name || '文件'}"`,
      );
      appendDownloadActions(transfer.display, payload);
      appendStatus(
        dom.fileStatus,
        `文件 "${name || transfer.meta.name || '文件'}" 已通过 SHA-256 校验：${shortHash(sha256)}。`,
      );
      triggerDownloadUrl(downloadUrl, name || transfer.meta.name || '接收文件');
      state.incomingTransfers.delete(key);
      break;
    }
    case 'large-file-error': {
      const { transferId, message: errorMessage, from } = payload;
      if (transferId) {
        const key = relayKey(transferId);
        const transfer = state.incomingTransfers.get(key);
        if (transfer?.display) {
          failTransferDisplay(transfer.display, errorMessage || '大文件传输失败。');
        }
        if (transfer) {
          state.incomingTransfers.delete(key);
        }
        const trackerInfo = state.transferTrackers.get(transferId);
        if (trackerInfo) {
          trackerInfo.tracker.cancelled = true;
          trackerInfo.tracker.cancelledReason = errorMessage || '大文件传输失败。';
          state.transferTrackers.delete(transferId);
        }
      }
      appendStatus(
        dom.fileStatus,
        `大文件传输失败：${peerLabel(from || payload?.targetId || '未知设备')} · ${errorMessage || '请稍后重试。'}`,
      );
      break;
    }
    case 'file-transfer-meta': {
      const { from, transferId, name, size, mime } = payload;
      if (!from || !transferId) {
        break;
      }
      const key = relayKey(transferId);
      const display = createTransferDisplay({
        direction: 'inbound',
        peerId: from,
        name,
        size,
      });
      state.incomingTransfers.set(key, {
        meta: { id: transferId, name, size, mime },
        chunks: [],
        receivedBytes: 0,
        display,
        peerId: from,
        mode: 'relay',
      });
      updateTransferDisplay(display, {
        percent: 0,
        status: `通过服务器接收「${name || '文件'}」…`,
      });
      appendStatus(
        dom.fileStatus,
        `正在通过服务器接收来自 ${peerLabel(from)} 的 "${name || '文件'}"（${humanFileSize(size)}）`,
      );
      break;
    }
    case 'file-transfer-chunk': {
      const { transferId, data: base64, size: totalSize } = payload;
      if (!transferId || typeof base64 !== 'string') {
        break;
      }
      const key = relayKey(transferId);
      const transfer = state.incomingTransfers.get(key);
      if (!transfer) {
        break;
      }
      const chunk = base64ToUint8Array(base64);
      transfer.chunks.push(chunk);
      transfer.receivedBytes += chunk.byteLength;
      const expectedSize = transfer.meta.size || totalSize || 0;
      const percent = expectedSize
        ? Math.min(100, Math.round((transfer.receivedBytes / expectedSize) * 100))
        : 0;
      if (transfer.display) {
        updateTransferDisplay(transfer.display, {
          percent,
          status: `通过服务器接收 ${percent}%`,
        });
      }
      break;
    }
    case 'file-transfer-complete': {
      const { transferId, name: friendlyName, mime } = payload;
      if (!transferId) {
        break;
      }
      const key = relayKey(transferId);
      const transfer = state.incomingTransfers.get(key);
      if (!transfer) {
        break;
      }
      const blob = new Blob(transfer.chunks, {
        type: mime || transfer.meta.mime || 'application/octet-stream',
      });
      const filename = friendlyName || transfer.meta.name || '接收文件';
      const downloadUrl = triggerDownload(blob, filename);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = filename;
      link.textContent = `重新下载 ${filename}`;
      link.className = 'download-link';
      link.rel = 'noopener';
      const actions = document.createElement('div');
      actions.className = 'transfer-actions';
      actions.appendChild(link);
      if (transfer.display) {
        completeTransferDisplay(
          transfer.display,
          `已通过服务器接收完成 "${filename}"，正在保存…`,
        );
        transfer.display.entry.appendChild(actions);
        updateStatusPanelScroll(dom.fileStatus);
      } else if (dom.fileStatus) {
        dom.fileStatus.appendChild(actions);
        updateStatusPanelScroll(dom.fileStatus);
      }
      appendStatus(dom.fileStatus, `文件 "${filename}" 已保存并可再次下载。`);
      state.incomingTransfers.delete(key);
      setTimeout(() => {
        URL.revokeObjectURL(downloadUrl);
        if (link.isConnected) {
          link.textContent = `${filename} 下载链接已过期`;
          link.removeAttribute('href');
          link.classList.add('download-link-disabled');
        }
      }, 5 * 60 * 1000);
      break;
    }
    case 'file-transfer-error': {
      const { transferId, message: errorMessage, from } = payload;
      if (!transferId) {
        break;
      }
      const key = relayKey(transferId);
      const transfer = state.incomingTransfers.get(key);
      let handled = false;
      if (transfer?.display) {
        failTransferDisplay(
          transfer.display,
          `服务器中转失败：${errorMessage || '对方已取消'}`,
        );
        handled = true;
      }
      if (transfer) {
        state.incomingTransfers.delete(key);
      }
      const trackerInfo = state.transferTrackers.get(transferId);
      if (trackerInfo) {
        trackerInfo.tracker.cancelled = true;
        trackerInfo.tracker.cancelledReason =
          errorMessage || '对方取消了接收。';
        state.transferTrackers.delete(transferId);
        handled = true;
      }
      const peerName = peerLabel(from || trackerInfo?.peerId || '未知设备');
      appendStatus(
        dom.fileStatus,
        `通过服务器的文件传输失败：${peerName} · ${errorMessage || '请稍后再试。'}`,
      );
      break;
    }
    case 'ping': {
      sendWsMessage('pong', { timestamp: Date.now() });
      break;
    }
    case 'error': {
      appendStatus(dom.fileStatus, `服务器错误：${payload.message}`);
      break;
    }
    default:
      break;
  }
};

const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsUrl = `${protocol}//${location.host}`;

const resetPeerState = () => {
  const peerIds = Array.from(state.peerConnections.keys());
  peerIds.forEach((peerId) => {
    cleanupPeer(peerId, { reason: 'reset' });
  });
  state.peers.clear();
  renderPeers();
};

const failAllTransfers = (message) => {
  const reason = message || '传输已终止。';
  for (const transfer of state.incomingTransfers.values()) {
    if (transfer.display) {
      failTransferDisplay(transfer.display, reason);
    }
  }
  state.incomingTransfers.clear();

  for (const transfer of state.outgoingTransfers.values()) {
    if (transfer.display) {
      failTransferDisplay(transfer.display, reason);
    }
  }
  state.outgoingTransfers.clear();

  for (const { tracker } of state.transferTrackers.values()) {
    if (tracker) {
      tracker.cancelled = true;
      tracker.cancelledReason = reason;
      if (tracker.display) {
        failTransferDisplay(tracker.display, reason);
      }
    }
  }
  state.transferTrackers.clear();
};

function clearReconnectTimer() {
  if (state.wsReconnectTimer) {
    clearTimeout(state.wsReconnectTimer);
    state.wsReconnectTimer = null;
  }
}

function scheduleReconnect() {
  if (state.wsReconnectTimer) return;
  const attempt = Math.max(0, state.wsReconnectAttempts);
  const delay = Math.min(WS_RECONNECT_BASE_DELAY * 2 ** attempt, WS_RECONNECT_MAX_DELAY);
  state.wsReconnectTimer = setTimeout(() => {
    state.wsReconnectTimer = null;
    connectWebSocket();
  }, delay);
  state.wsReconnectAttempts += 1;
}

function connectWebSocket() {
  const existing = state.ws;
  if (existing && (existing.readyState === WebSocket.OPEN || existing.readyState === WebSocket.CONNECTING)) {
    return;
  }

  clearReconnectTimer();

  const reconnecting = state.wsReconnectAttempts > 0 || Boolean(existing);
  if (reconnecting) {
    appendStatus(dom.fileStatus, '正在尝试重新连接发现服务器...');
  }
  setServerStatus(`连接中 · ${formatClock()}`);

  try {
    if (existing && existing.readyState === WebSocket.OPEN) {
      existing.close();
    }
  } catch {
    /* ignored */
  }

  const socket = new WebSocket(wsUrl);
  state.ws = socket;

  const handleOpen = () => {
    if (state.ws !== socket) return;
    state.wsReconnectAttempts = 0;
    clearReconnectTimer();
    appendStatus(dom.fileStatus, '已连接到发现服务器。');
    setServerStatus(`已连接 · ${formatClock()}`);
    const name = state.displayName || loadDisplayName();
    if (name) {
      state.displayName = name;
      if (dom.displayName) {
        dom.displayName.value = name;
      }
      if (dom.currentDisplayName) {
        dom.currentDisplayName.textContent = name;
      }
      sendWsMessage('register', { displayName: name });
      state.hasSentInitialRegister = true;
    } else {
      state.hasSentInitialRegister = false;
    }
  };

  const handleClose = () => {
    if (state.ws !== socket) return;
    state.ws = null;
    appendStatus(dom.fileStatus, '与服务器的连接已断开。');
    setServerStatus(`已断开 · ${formatClock()}`);
    state.hasSentInitialRegister = false;
    resetPeerState();
    failAllTransfers('服务器连接已断开。');
    scheduleReconnect();
  };

  const handleError = () => {
    if (state.ws !== socket) return;
    appendStatus(dom.fileStatus, 'WebSocket 连接出现错误。');
    setServerStatus(`连接异常 · ${formatClock()}`);
  };

  socket.addEventListener('open', handleOpen);
  socket.addEventListener('message', handleSocketMessage);
  socket.addEventListener('close', handleClose);
  socket.addEventListener('error', handleError);
}

const ensureWebSocket = () => {
  const socket = state.ws;
  if (!socket || socket.readyState === WebSocket.CLOSED) {
    connectWebSocket();
  }
};

document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    state.wsReconnectAttempts = 0;
    ensureWebSocket();
  }
});

window.addEventListener('online', () => {
  appendStatus(dom.fileStatus, '网络已恢复，正在重新连接服务器。');
  state.wsReconnectAttempts = 0;
  ensureWebSocket();
});

window.addEventListener('offline', () => {
  appendStatus(dom.fileStatus, '网络已断开，等待恢复后将自动重连。');
  setServerStatus(`离线 · ${formatClock()}`);
  clearReconnectTimer();
  state.wsReconnectAttempts = 0;
});

setServerStatus('正在连接...');
appendStatus(dom.fileStatus, '页面已刷新，开始初始化连接。');
connectWebSocket();

if (!('RTCPeerConnection' in window)) {
  appendStatus(dom.fileStatus, '此浏览器不支持 WebRTC。');
  dom.sendFileBtn.disabled = true;
}

if (!navigator.clipboard) {
  appendStatus(
    dom.clipboardStatus,
    '当前环境不支持剪贴板 API，请手动粘贴并复制内容。',
  );
} else {
  if (typeof navigator.clipboard.read !== 'function' || typeof ClipboardItem === 'undefined') {
    appendStatus(
      dom.clipboardStatus,
      '本浏览器暂不支持读取富文本剪贴板，仅能同步纯文本内容。',
    );
  }
  if (typeof navigator.clipboard.readText !== 'function') {
    appendStatus(
      dom.clipboardStatus,
      '此浏览器可能阻止读取剪贴板，如遇失败请手动粘贴后再分享。',
    );
  }
  if (typeof navigator.clipboard.write !== 'function' || typeof ClipboardItem === 'undefined') {
    appendStatus(
      dom.clipboardStatus,
      '本浏览器暂不支持写入富文本剪贴板，自动复制时将退回纯文本。',
    );
  }
  if (typeof navigator.clipboard.writeText !== 'function') {
    appendStatus(
      dom.clipboardStatus,
      '此浏览器可能阻止写入剪贴板，若自动复制失败请手动复制。',
    );
  }
}
