// Web Worker：在独立线程中对同一 IndexedDB 执行事务。
import { runTransaction, readCounter, reqToPromise } from './db.js';

const source = `worker-${Math.random().toString(36).slice(2, 6)}`;

function emit(event) {
  self.postMessage({ kind: 'event', event });
}

async function stressIncrement({ increments, strategy }) {
  for (let i = 0; i < increments; i++) {
    if (strategy === 'atomic') {
      await runTransaction({
        mode: 'readwrite',
        source,
        emit,
        op: { kind: 'increment' },
      });
    } else {
      // naive：读、写分两个事务，演示丢失更新
      const current = await readCounter();
      await runTransaction({
        mode: 'readwrite',
        source,
        emit,
        op: {
          kind: 'custom',
          custom: async ({ store }) => {
            await reqToPromise(store.put({ key: 'counter', value: current + 1, by: source }));
          },
        },
      });
    }
  }
  return increments;
}

self.onmessage = async (e) => {
  const msg = e.data;
  try {
    if (msg.cmd === 'run') {
      const result = await runTransaction({
        mode: msg.mode,
        source,
        emit,
        op: msg.op,
      });
      self.postMessage({ kind: 'done', jobId: msg.jobId, result });
    } else if (msg.cmd === 'stress') {
      const done = await stressIncrement(msg);
      self.postMessage({ kind: 'stressDone', jobId: msg.jobId, source, done });
    }
  } catch (err) {
    self.postMessage({ kind: 'failed', jobId: msg.jobId, error: String(err) });
  }
};

self.postMessage({ kind: 'ready', source });
