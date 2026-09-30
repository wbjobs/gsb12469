import { runTransaction, readCounter, resetCounter } from './db.js';
import { createBus } from './bus.js';
import { createTimeline } from './timeline.js';
import {
  runStressTest,
  verifyRollback,
  verifyConflict,
  verifyDeadlock,
  verifyDirtyRead,
} from './stress.js';

const tabId = `tab-${Math.random().toString(36).slice(2, 6)}`;
document.getElementById('tabId').textContent = tabId;

const $ = (id) => document.getElementById(id);
const logEl = $('log');
const timeline = createTimeline($('timeline'));

// ---------- 事件处理 ----------
function handleEvent(event) {
  if (event.wallTs == null) event.wallTs = Date.now();
  timeline.addEvent(event);
  appendLog(event);
}

function appendLog(ev) {
  const div = document.createElement('div');
  div.className = 'ev';
  const time = new Date(ev.wallTs).toLocaleTimeString('zh-CN', { hour12: false }) +
    '.' + String(ev.wallTs % 1000).padStart(3, '0');
  const cls = ev.type === 'commit' ? 'ok'
    : ev.type === 'abort' || ev.type === 'error' ? 'bad'
    : ev.type === 'conflict' ? 'warn'
    : `mode-${ev.mode || 'readonly'}`;
  div.innerHTML =
    `<span class="t">${time}</span>` +
    `<span class="src">[${ev.source || '?'}]</span>` +
    `<span class="${cls}">${ev.type}${ev.mode ? '·' + ev.mode : ''}</span> ` +
    `${ev.txId ? ev.txId + ' ' : ''}${ev.detail || ''}`;
  logEl.appendChild(div);
  while (logEl.children.length > 500) logEl.removeChild(logEl.firstChild);
  logEl.scrollTop = logEl.scrollHeight;
}

// 本地事件：上屏 + 广播给其他标签页
const bus = createBus(tabId, handleEvent);
function emit(event) {
  event.wallTs = Date.now();
  handleEvent(event);
  bus.post(event);
}

setInterval(() => {
  $('peerInfo').textContent = `在线标签页：${bus.peerCount()} 个（含本页）`;
}, 2000);

// ---------- Worker 池 ----------
let workers = [];
let jobSeq = 0;

function spawnWorkers(count) {
  for (const w of workers) w.worker.terminate();
  workers = [];
  for (let i = 0; i < count; i++) {
    const worker = new Worker('js/worker.js', { type: 'module' });
    const entry = { worker, source: null, pending: new Map() };
    worker.onmessage = (e) => {
      const msg = e.data;
      if (msg.kind === 'ready') {
        entry.source = msg.source;
        emit({ type: 'info', source: tabId, detail: `Worker ${msg.source} 已就绪` });
      } else if (msg.kind === 'event') {
        emit(msg.event); // 转发到本地时序图 + 其他标签页
      } else if (msg.kind === 'done' || msg.kind === 'stressDone') {
        const p = entry.pending.get(msg.jobId);
        if (p) { entry.pending.delete(msg.jobId); p.resolve(msg); }
      } else if (msg.kind === 'failed') {
        const p = entry.pending.get(msg.jobId);
        if (p) { entry.pending.delete(msg.jobId); p.reject(new Error(msg.error)); }
      }
    };
    entry.run = (payload) => new Promise((resolve, reject) => {
      const jobId = `job-${++jobSeq}`;
      entry.pending.set(jobId, { resolve, reject });
      worker.postMessage({ ...payload, jobId });
    });
    entry.stress = ({ increments, strategy }) =>
      entry.run({ cmd: 'stress', increments, strategy }).then((m) => m.done);
    workers.push(entry);
  }
  emit({ type: 'info', source: tabId, detail: `Worker 池已重建：${count} 个` });
}

$('btnSpawnWorkers').onclick = () => {
  spawnWorkers(Math.max(0, Math.min(8, Number($('workerCount').value) || 0)));
};
spawnWorkers(Number($('workerCount').value) || 2);

// ---------- 单次事务 ----------
$('btnRunOnce').onclick = async () => {
  const mode = $('txMode').value;
  const kind = $('opKind').value;
  const where = $('execWhere').value;
  const op = { kind, delayMs: kind === 'slowIncrement' ? 300 : 0 };
  if (where === 'worker') {
    if (workers.length === 0) {
      emit({ type: 'info', source: tabId, detail: '没有可用 Worker，请先重建 Worker 池' });
      return;
    }
    const w = workers[Math.floor(Math.random() * workers.length)];
    await w.run({ cmd: 'run', mode, op });
  } else {
    await runTransaction({ mode, source: tabId, emit, op });
  }
};

// ---------- 压力测试 ----------
$('btnStress').onclick = async () => {
  const btn = $('btnStress');
  btn.disabled = true;
  const resultEl = $('stressResult');
  resultEl.innerHTML = '<span class="info">压力测试进行中……</span>';
  try {
    const actors = Math.max(1, Number($('stressActors').value) || 4);
    const increments = Math.max(1, Number($('stressIncrements').value) || 50);
    const strategy = $('stressStrategy').value;
    const t0 = Date.now();
    const r = await runStressTest({ actors, increments, strategy, workers, emit });
    const elapsed = Date.now() - t0;
    const cls = r.pass ? 'pass' : 'fail';
    const verdict = r.pass
      ? `✔ 校验通过：最终值 ${r.actual} === 期望 ${r.expected}，无丢失更新 / 无脏读`
      : `✘ 校验失败：最终值 ${r.actual} ≠ 期望 ${r.expected}（丢失 ${r.expected - r.actual} 次更新）`;
    resultEl.innerHTML =
      `<span class="${cls}">${verdict}</span>\n` +
      `<span class="info">耗时 ${elapsed}ms，完成 ${r.totalDone} 次递增，策略=${strategy}。\n` +
      (strategy === 'naive'
        ? 'naive 策略把读和写拆成两个事务，丢失更新是预期行为——这正是需要单事务读-改-写的原因。'
        : 'atomic 策略在单个 readwrite 事务内读-改-写，IndexedDB 串行化同作用域写事务，结果必然精确。') +
      `</span>`;
  } catch (err) {
    resultEl.innerHTML = `<span class="fail">压力测试出错：${err}</span>`;
  } finally {
    btn.disabled = false;
  }
};

$('btnResetCounter').onclick = async () => {
  await resetCounter();
  emit({ type: 'info', source: tabId, detail: '计数器已重置为 0' });
};

// ---------- 隔离性验证 ----------
async function runVerify(btnId, fn) {
  const btn = $(btnId);
  btn.disabled = true;
  const el = $('verifyResult');
  el.innerHTML = '<span class="info">验证进行中……</span>';
  try {
    const r = await fn({ emit });
    el.innerHTML =
      `<span class="${r.pass ? 'pass' : 'fail'}">${r.pass ? '✔ 通过' : '✘ 未通过'}</span>\n` +
      `<span class="info">${r.detail}</span>`;
  } catch (err) {
    el.innerHTML = `<span class="fail">验证出错：${err}</span>`;
  } finally {
    btn.disabled = false;
  }
}

$('btnVerifyRollback').onclick = () => runVerify('btnVerifyRollback', verifyRollback);
$('btnVerifyConflict').onclick = () => runVerify('btnVerifyConflict', verifyConflict);
$('btnVerifyDeadlock').onclick = () => runVerify('btnVerifyDeadlock', verifyDeadlock);
$('btnVerifyDirtyRead').onclick = () => runVerify('btnVerifyDirtyRead', verifyDirtyRead);

// ---------- 时序图 ----------
$('btnClearLog').onclick = () => {
  logEl.innerHTML = '';
  timeline.clear();
};

(function loop() {
  timeline.draw({ followTail: $('followTail').checked });
  requestAnimationFrame(loop);
})();

emit({ type: 'info', source: tabId, detail: '页面已加载，数据库连接就绪' });
readCounter().then((v) =>
  emit({ type: 'info', source: tabId, detail: `当前 counter=${v}` }));
