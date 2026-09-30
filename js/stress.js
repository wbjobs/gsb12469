// 压力测试与隔离性验证场景。
import { runTransaction, readCounter, resetCounter, reqToPromise, openDb, STORE } from './db.js';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** 压力测试：actors 个执行者（1 个主线程 + N 个 Worker）并发递增计数器。 */
export async function runStressTest({ actors, increments, strategy, workers, emit, onProgress }) {
  await resetCounter();
  emit({ type: 'info', source: 'stress', detail: `计数器已重置，${actors} 个执行者 × ${increments} 次递增，策略=${strategy}` });

  const jobs = [];
  // 主线程承担 1 份，其余分给 Worker 池
  const workerJobs = Math.min(workers.length, actors - 1 >= 0 ? actors - 1 : 0);
  const tabShares = actors - workerJobs;

  for (let i = 0; i < tabShares; i++) {
    jobs.push((async () => {
      for (let k = 0; k < increments; k++) {
        if (strategy === 'atomic') {
          await runTransaction({ mode: 'readwrite', source: 'tab-stress', emit, op: { kind: 'increment' } });
        } else {
          const current = await readCounter();
          await runTransaction({
            mode: 'readwrite', source: 'tab-stress', emit,
            op: { kind: 'custom', custom: async ({ store }) => {
              await reqToPromise(store.put({ key: 'counter', value: current + 1, by: 'tab-stress' }));
            } },
          });
        }
      }
      return increments;
    })());
  }
  for (let i = 0; i < workerJobs; i++) {
    const w = workers[i % workers.length];
    jobs.push(w.stress({ increments, strategy }));
  }

  const results = await Promise.all(jobs);
  const totalDone = results.reduce((a, b) => a + b, 0);
  const expected = actors * increments;
  const actual = await readCounter();

  return {
    expected,
    actual,
    totalDone,
    pass: actual === expected,
    strategy,
  };
}

/** 验证 1：回滚不影响其他事务。 */
export async function verifyRollback({ emit }) {
  await resetCounter();
  const abortTx = runTransaction({
    mode: 'readwrite', source: 'verify', emit,
    op: { kind: 'writeAbort', delayMs: 100 },
  });
  await sleep(30); // 让 abort 事务先开始
  const commitTx = runTransaction({
    mode: 'readwrite', source: 'verify', emit,
    op: { kind: 'increment' },
  });
  const [a, b] = await Promise.all([abortTx, commitTx]);
  const final = await readCounter();
  const pass = a.aborted === true && b.committed === true && final === 1;
  return {
    pass, final,
    detail: `回滚事务 aborted=${a.aborted}，正常事务 committed=${b.committed}，最终值=${final}（期望 1：回滚的 +1 不生效，正常事务的 +1 生效）`,
  };
}

/** 验证 2：乐观锁冲突可检测（跨事务边界的读-改-写，用 version 字段检测）。 */
export async function verifyConflict({ emit }) {
  const db = await openDb();
  await new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, 'readwrite');
    tx.objectStore(STORE).put({ key: 'optimistic', value: 0, version: 1 });
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
  });

  async function optimisticIncrement(name) {
    // 第一步：readonly 读出当前版本
    const snap = await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readonly');
      const req = tx.objectStore(STORE).get('optimistic');
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    await sleep(150); // 人为放大竞争窗口，让两个事务交错
    // 第二步：readwrite 校验版本后写入
    let conflicted = false;
    await runTransaction({
      mode: 'readwrite', source: name, emit,
      op: {
        kind: 'custom', key: 'optimistic',
        custom: async ({ store, emit: txEmit, base }) => {
          const cur = await reqToPromise(store.get('optimistic'));
          if (cur.version !== snap.version) {
            conflicted = true;
            txEmit({ ...base, type: 'conflict', ts: performance.now(), detail: `版本 ${snap.version}→${cur.version}，检测到冲突，放弃写入` });
            throw new Error('optimistic-conflict');
          }
          await reqToPromise(store.put({ key: 'optimistic', value: cur.value + 1, version: cur.version + 1 }));
        },
      },
    });
    return { name, conflicted };
  }

  const [r1, r2] = await Promise.all([
    optimisticIncrement('verify-A'),
    optimisticIncrement('verify-B'),
  ]);
  const final = await new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, 'readonly');
    const req = tx.objectStore(STORE).get('optimistic');
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  const conflictCount = [r1, r2].filter((r) => r.conflicted).length;
  const pass = conflictCount === 1 && final.value === 1 && final.version === 2;
  return {
    pass, conflictCount, final,
    detail: `冲突检测次数=${conflictCount}（期望 1），最终 value=${final.value} version=${final.version}（期望 1/2：只有一个事务写入成功）`,
  };
}

/** 验证 3：交叉读写不死锁（IDB 通过事务串行化避免死锁）。 */
export async function verifyDeadlock({ emit }) {
  const db = await openDb();
  await new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, 'readwrite');
    const s = tx.objectStore(STORE);
    s.put({ key: 'lockA', value: 0 });
    s.put({ key: 'lockB', value: 0 });
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
  });

  const start = Date.now();
  const t1 = runTransaction({
    mode: 'readwrite', source: 'verify-X', emit,
    op: {
      kind: 'custom', key: 'lockA',
      custom: async ({ store, sleep: sl }) => {
        await reqToPromise(store.get('lockA'));
        await sl(200); // 模拟持锁期间的其他工作
        await reqToPromise(store.put({ key: 'lockB', value: 1 }));
      },
    },
  });
  const t2 = runTransaction({
    mode: 'readwrite', source: 'verify-Y', emit,
    op: {
      kind: 'custom', key: 'lockB',
      custom: async ({ store, sleep: sl }) => {
        await reqToPromise(store.get('lockB'));
        await sl(200);
        await reqToPromise(store.put({ key: 'lockA', value: 1 }));
      },
    },
  });

  const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000));
  try {
    await Promise.race([Promise.all([t1, t2]), timeout]);
    const elapsed = Date.now() - start;
    return {
      pass: true, elapsed,
      detail: `两个交叉读写事务均在 ${elapsed}ms 内提交（超时阈值 5000ms）。IndexedDB 对同一 object store 的 readwrite 事务串行执行，从机制上避免死锁。`,
    };
  } catch (err) {
    return { pass: false, detail: `疑似死锁：${err}` };
  }
}

/** 验证 4：并发写不脏读（写入延迟期间，并发只读事务只能看到旧值或新值，绝不看到中间态）。 */
export async function verifyDirtyRead({ emit }) {
  await resetCounter();
  const reads = [];
  let stop = false;

  const writer = runTransaction({
    mode: 'readwrite', source: 'verify-writer', emit,
    op: { kind: 'slowIncrement', delayMs: 500 },
  });

  const readers = (async () => {
    for (let i = 0; i < 12 && !stop; i++) {
      const v = await readCounter();
      reads.push(v);
      emit({ type: 'info', source: 'verify-reader', detail: `第 ${i + 1} 次读到 counter=${v}` });
      await sleep(60);
    }
  })();

  await writer;
  stop = true;
  await readers;
  const final = await readCounter();

  const allLegal = reads.every((v) => v === 0 || v === 1);
  let monotonic = true;
  for (let i = 1; i < reads.length; i++) {
    if (reads[i] < reads[i - 1]) monotonic = false;
  }
  const pass = allLegal && monotonic && final === 1;
  return {
    pass, reads, final,
    detail: `读序列=[${reads.join(', ')}]，全部 ∈ {0,1}=${allLegal}，单调不回退=${monotonic}，最终值=${final}（期望 1）。任何时刻都读不到未提交的中间值。`,
  };
}
