// 带事务插桩的 IndexedDB 封装，主线程与 Worker 共用。
export const DB_NAME = 'idb-concurrency-lab';
export const DB_VERSION = 1;
export const STORE = 'kv';

let dbPromise = null;
let txSeq = 0;

export function openDb() {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: 'key' });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  return dbPromise;
}

export function nextTxId(source) {
  txSeq += 1;
  return `${source}#${txSeq}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * 执行一个被插桩的事务。
 * op: { kind: 'read' | 'increment' | 'writeAbort' | 'slowIncrement' | 'custom',
 *       key, delayMs, abortAfterWrite, custom(tx, emit) }
 * emit(event) 回调：txStart / read / write / willAbort / commit / abort / error
 */
export async function runTransaction({ mode, op, source, emit }) {
  const db = await openDb();
  const txId = nextTxId(source);
  const key = op.key || 'counter';
  const base = { txId, source, mode, store: STORE, ts: performance.now() };

  emit({ ...base, type: 'txStart', detail: `key=${key} op=${op.kind}` });

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, mode);
    const store = tx.objectStore(STORE);
    let settled = false;

    tx.oncomplete = () => {
      settled = true;
      emit({ ...base, type: 'commit', ts: performance.now(), detail: '事务提交' });
      resolve({ txId, committed: true });
    };
    tx.onabort = () => {
      settled = true;
      emit({ ...base, type: 'abort', ts: performance.now(), detail: String(tx.error || '主动回滚') });
      resolve({ txId, committed: false, aborted: true });
    };
    tx.onerror = () => {
      emit({ ...base, type: 'error', ts: performance.now(), detail: String(tx.error) });
    };

    (async () => {
      try {
        if (op.kind === 'custom') {
          await op.custom({ tx, store, txId, emit, base, sleep });
          return;
        }
        const value = await reqToPromise(store.get(key));
        const current = value ? value.value : 0;
        emit({ ...base, type: 'read', ts: performance.now(), detail: `读到 ${key}=${current}` });

        if (op.kind === 'read') return;
        if (mode === 'readonly') {
          emit({ ...base, type: 'error', ts: performance.now(), detail: 'readonly 事务无法写入' });
          return;
        }
        if (op.delayMs) {
          emit({ ...base, type: 'wait', ts: performance.now(), detail: `人为延迟 ${op.delayMs}ms` });
          await sleep(op.delayMs);
        }
        const next = current + 1;
        await reqToPromise(store.put({ key, value: next, by: source, txId }));
        emit({ ...base, type: 'write', ts: performance.now(), detail: `写入 ${key}=${next}` });

        if (op.kind === 'writeAbort') {
          emit({ ...base, type: 'willAbort', ts: performance.now(), detail: '写入后主动 abort' });
          tx.abort();
        }
      } catch (err) {
        if (!settled) {
          emit({ ...base, type: 'error', ts: performance.now(), detail: String(err) });
          try { tx.abort(); } catch (_) { /* 已结束 */ }
        }
      }
    })();
  });
}

export function reqToPromise(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

export async function readCounter(key = 'counter') {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, 'readonly');
    const req = tx.objectStore(STORE).get(key);
    req.onsuccess = () => resolve(req.result ? req.result.value : 0);
    req.onerror = () => reject(req.error);
  });
}

export async function resetCounter(key = 'counter') {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, 'readwrite');
    tx.objectStore(STORE).put({ key, value: 0, by: 'reset' });
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}
