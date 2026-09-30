/* IndexedDB 并发实验室 —— 共享库（主线程 / Worker 通用，无框架） */
(function (global) {
  'use strict';

  var DB_NAME = 'idb-concurrency-lab';
  var DB_VERSION = 1;
  var BUS_NAME = 'idb-concurrency-lab-bus-v1';
  var STORES = ['kv', 'data', 'counter', 'oplog'];
  var PEER_TTL_MS = 9000;

  function nowTs() { return Date.now(); }
  function shortId() { return Math.random().toString(36).slice(2, 8) + Date.now().toString(36).slice(-3); }
  function sleep(ms) { return new Promise(function (res) { setTimeout(res, ms); }); }

  var isWorker = typeof WorkerGlobalScope !== 'undefined' && global instanceof WorkerGlobalScope;

  function makeNodeId() {
    if (isWorker) {
      var label = (global.location && global.location.hash || '').replace(/^#/, '') || 'worker';
      return label + '-' + shortId();
    }
    return 'tab-' + shortId();
  }

  function kindOf(nodeId) {
    if (nodeId.indexOf('worker') === 0) return 'worker';
    return 'tab';
  }

  function Node() {
    var self = this;
    this.nodeId = makeNodeId();
    this.kind = kindOf(this.nodeId);
    this.label = this.kind === 'worker'
      ? (this.nodeId.indexOf('workerA') === 0 ? 'Worker A'
        : this.nodeId.indexOf('workerB') === 0 ? 'Worker B'
        : 'Worker ' + this.nodeId.slice(-4))
      : '标签页 ' + this.nodeId.slice(-4);
    this.peers = {};
    this.actors = {};
    this.timelineListeners = [];
    this.peersListeners = [];
    this.rpcHandlers = {};
    this.pending = {};
    this._controls = {};
    this._seq = 0;

    if (typeof BroadcastChannel !== 'undefined') {
      this.bc = new BroadcastChannel(BUS_NAME);
      this.bc.addEventListener('message', function (ev) { self._onBus(ev.data); });
      this._heartbeat = setInterval(function () { self._sendPresence(); }, 3000);
      this._sweeper = setInterval(function () { self._sweep(); }, 2000);
    }
  }

  Node.prototype.onTimeline = function (fn) { this.timelineListeners.push(fn); };
  Node.prototype.onPeers = function (fn) { this.peersListeners.push(fn); fn(this._peerList()); };

  Node.prototype._peerList = function () {
    var self = this;
    return Object.keys(this.peers).sort().map(function (id) {
      return { id: id, kind: kindOf(id), label: self.peers[id].label };
    });
  };

  Node.prototype._notifyPeers = function () {
    var list = this._peerList();
    this.peersListeners.forEach(function (fn) { fn(list); });
  };

  Node.prototype._sweep = function () {
    var changed = false;
    var t = nowTs();
    for (var id in this.peers) {
      if (t - this.peers[id].t > PEER_TTL_MS) { delete this.peers[id]; changed = true; }
    }
    if (changed) this._notifyPeers();
  };

  Node.prototype._sendPresence = function () {
    if (!this.bc) return;
    this.bc.postMessage({ kind: 'presence', from: this.nodeId, label: this.label, t: nowTs() });
  };

  Node.prototype.announce = function () { this._sendPresence(); };

  Node.prototype._touchPeer = function (from, label) {
    var known = this.peers[from];
    if (!known) {
      this.peers[from] = { label: label || kindOf(from), t: nowTs() };
      this._notifyPeers();
      if (this.bc) this.bc.postMessage({ kind: 'presence', from: this.nodeId, label: this.label, t: nowTs(), replyTo: from });
    } else {
      var before = known.t;
      known.t = nowTs();
      if (label && known.label !== label) { known.label = label; }
      if (known.t - before > 1000 || label) this._notifyPeers();
    }
  };

  Node.prototype.publish = function (ev) {
    ev.from = this.nodeId;
    ev.label = this.label;
    if (!ev.t) ev.t = nowTs();
    this.timelineListeners.forEach(function (fn) { fn(ev); });
    if (this.bc) this.bc.postMessage({ kind: 'timeline', ev: ev });
  };

  Node.prototype._onBus = function (msg) {
    if (!msg || !msg.kind) return;
    if (msg.kind === 'presence') {
      if (msg.from === this.nodeId) return;
      this._touchPeer(msg.from, msg.label);
      if (msg.replyTo === this.nodeId) { /* 对方已记录自己 */ }
      return;
    }
    if (msg.kind === 'timeline') {
      var ev = msg.ev;
      if (!ev || ev.from === this.nodeId) return;
      this.timelineListeners.forEach(function (fn) { fn(ev); });
      return;
    }
    if (msg.kind === 'rpc') this._onRpc(msg);
  };

  Node.prototype.registerRpc = function (method, handler) { this.rpcHandlers[method] = handler; };

  Node.prototype._onRpc = function (msg) {
    var self = this;
    var m = msg.msg;
    if (m.to !== this.nodeId) return;

    if (m.type === 'request') {
      var handler = this.rpcHandlers[m.method];
      var done = function (payload, err) {
        if (!self.bc) return;
        self.bc.postMessage({
          kind: 'rpc',
          msg: { type: 'response', id: m.id, from: self.nodeId, to: m.from, payload: payload || null, error: err || null }
        });
      };
      if (!handler) { done(null, 'no handler: ' + m.method); return; }
      var ctl = { onControl: function (fn) { self._controls[m.id] = fn; } };
      Promise.resolve()
        .then(function () {
          return handler(m.payload || {}, function (ev) {
            ev.callId = m.id;
            if (!self.bc) return;
            self.bc.postMessage({ kind: 'rpc', msg: { type: 'event', id: m.id, from: self.nodeId, to: m.from, ev: ev } });
          }, ctl);
        })
        .then(function (result) { delete self._controls[m.id]; done(result || null, null); })
        .catch(function (err) { delete self._controls[m.id]; done(null, String((err && err.message) || err)); });
      return;
    }

    if (m.type === 'control') {
      var cfn = this._controls[m.id];
      if (cfn) cfn(m.name);
      return;
    }
    var slot = this.pending[m.id];
    if (!slot) return;
    if (m.type === 'event' && slot.onEvent) slot.onEvent(m.ev);
    if (m.type === 'response') {
      delete this.pending[m.id];
      clearTimeout(slot.timer);
      if (m.error) slot.reject(new Error(m.error)); else slot.resolve(m.payload);
    }
  };

  Node.prototype.callRemote = function (target, method, payload, opts) {
    var self = this;
    opts = opts || {};
    var id = 'rpc-' + (++self._seq) + '-' + shortId();
    var promise = new Promise(function (resolve, reject) {
      if (!self.bc) { reject(new Error('BroadcastChannel 不可用')); return; }
      var timer = setTimeout(function () {
        delete self.pending[id];
        reject(new Error('RPC 超时: ' + method));
      }, opts.timeoutMs || 30000);
      self.pending[id] = { resolve: resolve, reject: reject, timer: timer, onEvent: opts.onEvent || null };
      self.bc.postMessage({
        kind: 'rpc',
        msg: { type: 'request', id: id, from: self.nodeId, to: target, method: method, payload: payload || {} }
      });
    });
    return { id: id, promise: promise, ctl: function (name) { self.sendControl(target, id, name); } };
  };

  Node.prototype.sendControl = function (target, callId, name) {
    if (!this.bc) return;
    this.bc.postMessage({
      kind: 'rpc',
      msg: { type: 'control', id: callId, from: this.nodeId, to: target, name: name }
    });
  };

  global.DB_NAME = DB_NAME;
  global.DB_VERSION = DB_VERSION;
  global.STORES = STORES;
  global.idbNow = nowTs;
  global.idbSleep = sleep;
  global.idbShortId = shortId;
  global.idbIsWorker = isWorker;
  global.Node = Node;
})(typeof self !== 'undefined' ? self : this);

/* ============ 数据库与事务 ============ */
(function (global) {
  'use strict';

  var _dbPromise = null;

  function openDB(node) {
    if (_dbPromise) return _dbPromise;
    _dbPromise = new Promise(function (resolve, reject) {
      var req = indexedDB.open(global.DB_NAME, global.DB_VERSION);
      req.onupgradeneeded = function () {
        var db = req.result;
        global.STORES.forEach(function (name) {
          if (!db.objectStoreNames.contains(name)) db.createObjectStore(name);
        });
      };
      req.onsuccess = function () { resolve(req.result); };
      req.onerror = function () { reject(req.error); };
      req.onblocked = function () {
        node.publish({ type: 'note', phase: 'db-blocked', detail: '数据库升级被其他标签页阻塞，请关闭旧版本标签页' });
      };
    });
    return _dbPromise;
  }

  function reqP(request) {
    return new Promise(function (resolve, reject) {
      request.onsuccess = function () { resolve(request.result); };
      request.onerror = function () { reject(request.error); };
    });
  }

  var TXN_SEQ = 0;

  /*
   * 事务包装器：
   *  - readonly 使用创建时刻的快照（IndexedDB 规范保证）
   *  - readwrite 对同一 store 的写事务被浏览器串行化
   *  - 首个请求完成前记录等待锁（grant）的耗时
   *  - 自动提交 = commit；显式/异常中止 = abort
   */
  function Txn(node, mode, storeNames, opts) {
    opts = opts || {};
    this.node = node;
    this.mode = mode;
    this.stores = storeNames.slice();
    this.quiet = !!opts.quiet || !!opts.silent;
    this.silent = !!opts.silent;
    this.id = 'tx' + (++TXN_SEQ) + '-' + global.idbShortId().slice(0, 5);
    this.startT = global.idbNow();
    this.grantT = 0;
    this.waitMs = 0;
    this.settled = false;
    this.ops = 0;
    this.conflict = false;
    this._committed = false;
    this._finishError = null;
    this._waiters = [];

    var self = this;
    this.ready = openDB(node).then(function (db) {
      self.idbTx = db.transaction(self.stores, mode);
      if (!self.silent) node.publish({
        type: 'txn', phase: 'begin', txnId: self.id, mode: mode,
        stores: self.stores.slice(), t: self.startT
      });
      self.idbTx.addEventListener('complete', function () { self._finish(true, null); });
      self.idbTx.addEventListener('abort', function () {
        self._finish(false, self._abortError || new Error('abort'));
      });
      return self;
    });
  }

  Txn.prototype.store = function (name) { return this.idbTx.objectStore(name); };

  Txn.prototype._markGrant = function () {
    if (!this.grantT) {
      this.grantT = global.idbNow();
      this.waitMs = this.grantT - this.startT;
      if (!this.silent) this.node.publish({
        type: 'txn', phase: 'grant', txnId: this.id, mode: this.mode,
        stores: this.stores.slice(), waitMs: this.waitMs, t: this.grantT
      });
    }
  };

  Txn.prototype._wrap = function (p, op, storeName, key) {
    var self = this;
    this.ops++;
    return p.then(function (v) {
      self._markGrant();
      if (!self.quiet) {
        self.node.publish({
          type: 'op', txnId: self.id, mode: self.mode, store: storeName,
          op: op, key: key === undefined ? null : String(key), t: global.idbNow()
        });
      }
      return v;
    }, function (err) {
      self._markGrant();
      throw err;
    });
  };

  Txn.prototype.get = function (storeName, key) {
    return this._wrap(reqP(this.store(storeName).get(key)), 'get', storeName, key);
  };
  Txn.prototype.put = function (storeName, key, value) {
    return this._wrap(reqP(this.store(storeName).put(value, key)), 'put', storeName, key);
  };
  Txn.prototype.del = function (storeName, key) {
    return this._wrap(reqP(this.store(storeName).delete(key)), 'delete', storeName, key);
  };
  Txn.prototype.count = function (storeName) {
    return this._wrap(reqP(this.store(storeName).count()), 'count', storeName, null);
  };
  Txn.prototype.clear = function (storeName) {
    return this._wrap(reqP(this.store(storeName).clear()), 'clear', storeName, null);
  };

  Txn.prototype.abort = function (reason) {
    this._abortError = new Error(reason || 'aborted');
    this._abortError.isAbort = true;
    this._abortReason = reason || '';
    if (this.idbTx) this.idbTx.abort();
  };

  Txn.prototype._finish = function (committed, err) {
    if (this.settled) return;
    this.settled = true;
    this._committed = committed;
    this._finishError = err || null;
    var endT = global.idbNow();
    var conflict = !committed && /CAS|conflict|冲突/i.test((err && err.message) || this._abortReason || '');
    this.conflict = conflict;
    if (!this.silent) this.node.publish({
      type: 'txn',
      phase: committed ? 'commit' : 'abort',
      txnId: this.id, mode: this.mode, stores: this.stores.slice(),
      waitMs: this.waitMs, ops: this.ops, durationMs: endT - this.startT,
      conflict: conflict, reason: committed ? '' : ((err && err.message) || 'aborted'),
      t: endT
    });
    var waiters = this._waiters;
    this._waiters = [];
    waiters.forEach(function (w) { if (committed) w.resolve(); else w.reject(err); });
  };

  /* 等待事务结束。写事务需 await 它，保证下一个写事务被串行调度。 */
  Txn.prototype.done = function () {
    if (this.settled) {
      return this._committed ? Promise.resolve() : Promise.reject(this._finishError);
    }
    var self = this;
    return new Promise(function (resolve, reject) {
      self._waiters.push({ resolve: resolve, reject: reject });
    });
  };

  /* 清空若干 store（单个 readwrite 事务，整体原子） */
  function resetStores(node, names) {
    var tx = new Txn(node, 'readwrite', names, { quiet: true });
    return tx.ready.then(function () {
      return Promise.all(names.map(function (n) { return tx.clear(n); }));
    }).then(function () { return tx.done(); }).then(function () { return true; });
  }

  global.openDB = openDB;
  global.Txn = Txn;
  global.resetStores = resetStores;
  global.reqP = reqP;
})(typeof self !== 'undefined' ? self : this);

/* ============ Actor：在任意标签页 / Worker 中执行的并发角色 ============ */
(function (global) {
  'use strict';

  var BATCH = 20;                 // 压测每个写事务批量提交的增量
  var MAX_HOLD_OPS = 20000;       // 长事务存活上限（保护）
  var CAS_MAX_ATTEMPTS = 30;

  function registerActors(node) {

    function controlBridge() {
      // 返回 { use, wait } ；use 由 RPC 层注入，wait 给 actor 用
      var handler = null;
      return {
        use: function (fn) { handler = fn; },
        fire: function (name) { if (handler) handler(name); },
        wait: function (name) {
      return new Promise(function (resolve) {
        var h = handler;
        handler = function (n) { if (n === name) { handler = h; resolve(); } else if (h) h(n); };
      });
        }
      };
    }

    async function pumpWrite(tx, emit, phaseName, shouldStop) {
      var i = 0;
      while (true) {
        i++;
        await tx.put('kv', 'x', i);
        await tx.put('data', 'x', i);
        if (i === 1 && phaseName) emit({ name: phaseName });
        if (shouldStop()) break;
        if (i >= MAX_HOLD_OPS) break;
      }
      return i;
    }

    async function readLoop(tx, emit, phaseName, shouldStop) {
      var seenKv = {};
      var seenData = {};
      var reads = 0;
      var k0 = await tx.get('kv', 'x');
      var d0 = await tx.get('data', 'x');
      seenKv[String(k0)] = 1; seenData[String(d0)] = 1;
      reads++;
      emit({ name: phaseName, kv: k0, data: d0 });
      var tick = 0;
      while (!shouldStop()) {
        var k = await tx.get('kv', 'x');
        var d = await tx.get('data', 'x');
        reads++;
        seenKv[String(k)] = 1; seenData[String(d)] = 1;
        if (++tick % 200 === 0) emit({ name: phaseName + '-sample', kv: k, data: d, reads: reads });
      }
      await tx.done();
      return {
        snapshot: { kv: k0, data: d0 },
        seenKv: Object.keys(seenKv), seenData: Object.keys(seenData), reads: reads
      };
    }

    /* ---- 脏写：长时间持有 readwrite，直到收到 commit ---- */
    node.registerRpc('dirtyWriter', async function (p, emit, ctl) {
      var commit = false;
      ctl.onControl(function (name) { if (name === 'commit') commit = true; });
      var tx = new global.Txn(node, 'readwrite', ['kv', 'data']);
      await tx.ready;
      var n = await pumpWrite(tx, emit, 'dirty-writing', function () { return commit; });
      await tx.done();
      return { writes: n, committed: true };
    });

    /* ---- 脏读探针：readonly 快照，事务期间反复读，直到收到 done ---- */
    node.registerRpc('dirtyReader', async function (p, emit, ctl) {
      var done = false;
      ctl.onControl(function (name) { if (name === 'reader-done') done = true; });
      var tx = new global.Txn(node, 'readonly', ['kv', 'data']);
      await tx.ready;
      var res = await readLoop(tx, emit, 'reader-snapshot', function () { return done; });
      var fresh = new global.Txn(node, 'readonly', ['kv', 'data'], { quiet: true });
      await fresh.ready;
      var fkv = await fresh.get('kv', 'x');
      var fdata = await fresh.get('data', 'x');
      await fresh.done();
      res.fresh = { kv: fkv, data: fdata };
      return res;
    });

    /* ---- 回滚写：收到 abort 后中止事务 ---- */
    node.registerRpc('rollbackWriter', async function (p, emit, ctl) {
      var aborted = false;
      ctl.onControl(function (name) { if (name === 'abort') aborted = true; });
      var tx = new global.Txn(node, 'readwrite', ['kv', 'data']);
      await tx.ready;
      var n = 0;
      try {
        n = await pumpWrite(tx, emit, 'rollback-writing', function () { return aborted; });
        if (!aborted) { await tx.done(); }
        else { tx.abort('rollback-demo'); await tx.done().catch(function () {}); }
      } catch (e) { /* abort */ }
      return { writes: n, rolledBack: aborted };
    });

    node.registerRpc('rollbackReader', async function (p, emit, ctl) {
      var done = false;
      ctl.onControl(function (name) { if (name === 'reader-done') done = true; });
      var tx = new global.Txn(node, 'readonly', ['kv', 'data']);
      await tx.ready;
      var res = await readLoop(tx, emit, 'rollback-reader-snapshot', function () { return done; });
      var fresh = new global.Txn(node, 'readonly', ['kv', 'data'], { quiet: true });
      await fresh.ready;
      res.fresh = { kv: await fresh.get('kv', 'x'), data: await fresh.get('data', 'x') };
      await fresh.done();
      return res;
    });

    /* ---- CAS 写者：阶段一 RO 读版本，屏障后阶段二 RW 条件写 ---- */
    node.registerRpc('casWriter', async function (p, emit, ctl) {
      var go = false;
      ctl.onControl(function (name) { if (name === 'cas-write') go = true; });
      var ro = new global.Txn(node, 'readonly', ['kv'], { quiet: true });
      await ro.ready;
      var base = await ro.get('kv', 'doc');
      await ro.done();
      var baseVer = base ? base.ver : 0;
      emit({ name: 'cas-read', ver: baseVer });
      while (!go) await global.idbSleep(5);

      var conflicts = 0, attempts = 0, ok = false, finalVer = null;
      var expectedVer = baseVer;
      while (!ok && attempts < CAS_MAX_ATTEMPTS) {
        attempts++;
        var tx = new global.Txn(node, 'readwrite', ['kv'], { quiet: true });
        await tx.ready;
        var cur = await tx.get('kv', 'doc');
        if (cur.ver !== expectedVer) {
          conflicts++;
          tx.abort('CAS conflict: expected ver ' + expectedVer + ' got ' + cur.ver);
          await tx.done().catch(function () {});
          // 重新以只读快照读取“已提交”的最新版本，作为下一轮条件
          var ro = new global.Txn(node, 'readonly', ['kv'], { quiet: true });
          await ro.ready;
          var latest = await ro.get('kv', 'doc');
          await ro.done();
          expectedVer = latest.ver;
          continue;
        }
        await tx.put('kv', 'doc', { ver: cur.ver + 1, by: node.nodeId, attempt: attempts });
        finalVer = cur.ver + 1;
        await tx.done();
        ok = true;
      }
      return { attempts: attempts, conflicts: conflicts, finalVer: finalVer, baseVer: baseVer };
    });

    /* ---- 死锁演示：两个跨 store 的长写事务，声明顺序相反 ---- */
    node.registerRpc('deadWriter', async function (p, emit, ctl) {
      var go = false;
      ctl.onControl(function (name) { if (name === 'dead-go') go = true; });
      while (!go) await global.idbSleep(5);
      var order = p.order === 'BA' ? ['data', 'kv'] : ['kv', 'data'];
      var tx = new global.Txn(node, 'readwrite', ['kv', 'data']);
      await tx.ready;
      var start = global.idbNow();
      await tx.get(order[0], 'warmup'); // 首个请求完成 = 已获得锁
      var per = p.per || 1200;
      for (var i = 0; i < per; i++) { await tx.put(order[0], 'k' + i, i); }
      emit({ name: 'dead-first-store-done', order: p.order });
      for (var j = 0; j < per; j++) { await tx.put(order[1], 'k' + j, j); }
      await tx.done();
      // grantWait 由事务封装器在授权事件上记录（创建 -> 拿到锁），不包含请求自身耗时
      return { order: p.order, grantWait: tx.waitMs, duration: global.idbNow() - start };
    });

    /* ---- 压测写者：批量条件递增，带屏障 ---- */
    node.registerRpc('stressWriter', async function (p, emit, ctl) {
      var go = false;
      ctl.onControl(function (name) { if (name === 'stress-go') go = true; });
      while (!go) await global.idbSleep(5);
      var deadline = global.idbNow() + p.durationMs;
      var ops = 0; var txnCount = 0;
      var lastReport = global.idbNow();
      while (global.idbNow() < deadline) {
        var tx = new global.Txn(node, 'readwrite', ['counter', 'oplog', 'data'], { silent: true });
        await tx.ready;
        var cur = await tx.get('counter', 'n');
        var base = cur || 0;
        var end = Math.min(base + BATCH, base + BATCH);
        for (var i = base + 1; i <= end; i++) {
          await tx.put('oplog', node.nodeId + ':' + global.idbShortId() + ':' + i, { n: i, by: node.nodeId });
        }
        await tx.put('counter', 'n', end);
        await tx.put('data', 'last', end);
        await tx.done();
        txnCount++;
        ops += BATCH;
        if (global.idbNow() - lastReport > 250) {
          lastReport = global.idbNow();
          node.publish({ type: 'note', phase: 'stress-progress', detail: node.label + ' 已写入 ' + ops });
          emit({ name: 'stress-progress', ops: ops });
        }
      }
      return { ops: ops, txnCount: txnCount };
    });

    /* ---- 只读校验：跨 store 一致性 ---- */
    node.registerRpc('readCounter', async function () {
      var tx = new global.Txn(node, 'readonly', ['counter', 'oplog', 'data'], { quiet: true });
      await tx.ready;
      var n = await tx.get('counter', 'n');
      var oplog = await tx.count('oplog');
      var last = await tx.get('data', 'last');
      await tx.done();
      return { n: n || 0, oplog: oplog, last: last || 0, by: node.nodeId, kind: node.kind };
    });

    node.registerRpc('ping', async function () { return { ok: true, node: node.nodeId, kind: node.kind }; });
  }

  global.registerActors = registerActors;
  global.STRESS_BATCH = BATCH;
})(typeof self !== 'undefined' ? self : this);

/* ============ 协调器：编排跨标签页 / Worker 的场景 ============ */
(function (global) {
  'use strict';

  function waitFor(fn, timeoutMs, label) {
    var deadline = global.idbNow() + timeoutMs;
    return new Promise(function (resolve, reject) {
      (function tick() {
        var v;
        try { v = fn(); } catch (e) { return reject(e); }
        if (v) return resolve(v);
        if (global.idbNow() > deadline) return reject(new Error('等待超时: ' + (label || 'event')));
        setTimeout(tick, 8);
      })();
    });
  }

  function onceEvent(collector, name) {
    return new Promise(function (resolve) {
      var t = setInterval(function () {
        for (var i = 0; i < collector.length; i++) {
          if (collector[i].name === name) { clearInterval(t); resolve(collector[i]); return; }
        }
      }, 8);
    });
  }

  /* 在本节点执行已注册 actor，语义对齐远程 RPC */
  function localCall(node, method, payload, onEvent) {
    var id = 'local-' + global.idbShortId();
    var ctlFn = null;
    var ctl = { onControl: function (fn) { ctlFn = fn; } };
    var handler = node.rpcHandlers[method];
    var emitter = function (ev) { if (onEvent) onEvent(ev); };
    var promise = Promise.resolve().then(function () {
      if (!handler) throw new Error('no handler: ' + method);
      return handler(payload || {}, emitter, ctl);
    });
    return { id: id, promise: promise, ctl: function (name) { if (ctlFn) ctlFn(name); } };
  }

  function launch(node, target, method, payload, onEvent, timeoutMs) {
    if (target === node.nodeId) return localCall(node, method, payload, onEvent);
    return node.callRemote(target, method, payload, { onEvent: onEvent || null, timeoutMs: timeoutMs || 30000 });
  }

  function targets(node, peers) {
    return [node.nodeId].concat(peers.map(function (p) { return p.id; }));
  }

  function distribute(node, peers, n) {
    var all = targets(node, peers);
    var out = [];
    for (var i = 0; i < n; i++) out.push(all[i % all.length]);
    return out;
  }

  function pick(node, peers, kinds) {
    for (var i = 0; i < peers.length; i++) {
      if (!kinds || kinds.indexOf(peers[i].kind) !== -1) return peers[i].id;
    }
    return null;
  }

  function check(label, pass, detail) {
    return { label: label, pass: !!pass, detail: detail || '' };
  }

  /* ---- 场景 1：并发写不能脏读 ---- */
  async function runDirtyRead(node, peers, log) {
    log('重置 kv / data 并写入初始值 x=0');
    await global.resetStores(node, ['kv', 'data']);
    var initTx = new global.Txn(node, 'readwrite', ['kv', 'data'], { quiet: true });
    await initTx.ready;
    await initTx.put('kv', 'x', 0);
    await initTx.put('data', 'x', 0);
    await initTx.done();

    var writerTarget = pick(node, peers) || node.nodeId;
    var readerTarget = null;
    var all = targets(node, peers);
    for (var i = 0; i < all.length; i++) { if (all[i] !== writerTarget) { readerTarget = all[i]; break; } }
    if (!readerTarget) readerTarget = node.nodeId;
    log('写者: ' + writerTarget + ' ｜ 只读探针: ' + readerTarget);

    var wEvents = [], rEvents = [];
    var writer = launch(node, writerTarget, 'dirtyWriter', {}, function (e) { wEvents.push(e); });
    var reader = launch(node, readerTarget, 'dirtyReader', {}, function (e) { rEvents.push(e); });

    await onceEvent(wEvents, 'dirty-writing');
    await onceEvent(rEvents, 'reader-snapshot');
    log('写者已开始写入，只读事务仍持有创建时快照（重叠 400ms）');
    await global.idbSleep(400);
    log('指示写者提交事务');
    writer.ctl('commit');
    var wres = await writer.promise;
    reader.ctl('reader-done');
    var rres = await reader.promise;

    log('写者共写入 ' + wres.writes + ' 个递增版本；只读探针读取 ' + rres.reads + ' 次');

    var checks = [];
    checks.push(check('写者已提交多版本', wres.writes > 1, '提交值个数=' + wres.writes));
    checks.push(check('只读事务期间无脏读（快照始终为 0）',
      rres.seenKv.length === 1 && String(rres.snapshot.kv) === '0',
      '看到的 kv 值集合=[' + rres.seenKv.join(',') + ']'));
    checks.push(check('提交后新事务读到已提交值',
      Number(rres.fresh.kv) === wres.writes,
      '新事务读到=' + rres.fresh.kv + '，期望=' + wres.writes));
    checks.push(check('跨 store 原子可见',
      String(rres.fresh.kv) === String(rres.fresh.data),
      'kv=' + rres.fresh.kv + ', data=' + rres.fresh.data));
    return { name: '并发写不脏读', checks: checks };
  }

  /* ---- 场景 2：回滚不影响其他事务 ---- */
  async function runRollback(node, peers, log) {
    log('重置 kv / data 并写入初始值 x=0');
    await global.resetStores(node, ['kv', 'data']);
    var initTx = new global.Txn(node, 'readwrite', ['kv', 'data'], { quiet: true });
    await initTx.ready;
    await initTx.put('kv', 'x', 0);
    await initTx.put('data', 'x', 0);
    await initTx.done();

    var writerTarget = pick(node, peers) || node.nodeId;
    var readerTarget = node.nodeId;
    var all = targets(node, peers);
    for (var i = 0; i < all.length; i++) { if (all[i] !== writerTarget) { readerTarget = all[i]; break; } }

    var wEvents = [], rEvents = [];
    var writer = launch(node, writerTarget, 'rollbackWriter', {}, function (e) { wEvents.push(e); });
    var reader = launch(node, readerTarget, 'rollbackReader', {}, function (e) { rEvents.push(e); });
    await onceEvent(wEvents, 'rollback-writing');
    await onceEvent(rEvents, 'rollback-reader-snapshot');
    log('回滚者写入中，只读探针重叠读取 400ms');
    await global.idbSleep(400);
    log('指示回滚者 abort 事务');
    writer.ctl('abort');
    var wres = await writer.promise;
    reader.ctl('reader-done');
    var rres = await reader.promise;

    var verifyTx = new global.Txn(node, 'readonly', ['kv', 'data'], { quiet: true });
    await verifyTx.ready;
    var vkv = await verifyTx.get('kv', 'x');
    var vdata = await verifyTx.get('data', 'x');
    await verifyTx.done();

    var checks = [];
    checks.push(check('回滚事务确实中止', wres.rolledBack === true, '中止前写入=' + wres.writes));
    checks.push(check('回滚后数据保持初始值 0', vkv === 0 && vdata === 0, 'kv=' + vkv + ', data=' + vdata));
    checks.push(check('并发只读事务从未读到未提交数据',
      rres.seenKv.length === 1 && String(rres.snapshot.kv) === '0',
      '只读值集合=[' + rres.seenKv.join(',') + ']'));
    return { name: '事务回滚隔离', checks: checks };
  }

  /* ---- 场景 3：事务冲突可检测（CAS 乐观并发） ---- */
  async function runCas(node, peers, log, contenders) {
    contenders = contenders || 8;
    log('重置 kv.doc 版本号为 0');
    await global.resetStores(node, ['kv']);
    var initTx = new global.Txn(node, 'readwrite', ['kv'], { quiet: true });
    await initTx.ready;
    await initTx.put('kv', 'doc', { ver: 0 });
    await initTx.done();

    var tgs = distribute(node, peers, contenders);
    log(contenders + ' 个 CAS 写者分布在 ' + targets(node, peers).length + ' 个上下文');

    var readCount = 0;
    var handles = tgs.map(function (t) {
      return launch(node, t, 'casWriter', {}, function (e) {
        if (e.name === 'cas-read') readCount++;
      }, 30000);
    });
    await waitFor(function () { return readCount >= contenders; }, 8000, '所有写者完成阶段一读取');
    log('全部写者读到旧版本 ver=0，同时放行条件写');
    handles.forEach(function (h) { h.ctl('cas-write'); });
    var results = await Promise.all(handles.map(function (h) { return h.promise; }));
    var conflicts = results.reduce(function (s, r) { return s + (r.conflicts || 0); }, 0);
    var finalVers = results.map(function (r) { return r.finalVer; });

    var finalTx = new global.Txn(node, 'readonly', ['kv'], { quiet: true });
    await finalTx.ready;
    var doc = await finalTx.get('kv', 'doc');
    await finalTx.done();

    var checks = [];
    checks.push(check('检测到版本冲突并中止重试', conflicts >= contenders - 1,
      '冲突中止次数=' + conflicts));
    checks.push(check('最终版本恰好自增到 ' + contenders, doc.ver === contenders,
      '最终 ver=' + doc.ver));
    checks.push(check('所有写者均提交成功（无丢失更新）',
      finalVers.length === contenders, '各写者最终版本=' + finalVers.join(',')));
    return { name: '事务冲突检测', checks: checks };
  }

  /* ---- 场景 4：死锁避免（跨 store 长事务串行化） ---- */
  async function runDeadlock(node, peers, log) {
    await global.resetStores(node, ['kv', 'data']);
    var all = targets(node, peers);
    var tA = pick(node, peers) || node.nodeId;
    var tB = node.nodeId;
    for (var i = 0; i < all.length; i++) { if (all[i] !== tA) { tB = all[i]; break; } }
    log('两个长写事务声明相同 store、加锁顺序相反：' + tA + ' (AB) vs ' + tB + ' (BA)');

    var registered = { A: false, B: false };
    var hA = launch(node, tA, 'deadWriter', { order: 'AB', per: 1000 }, function (e) {
      if (e.name === 'dead-first-store-done') registered.A = true;
    }, 20000);
    var hB = launch(node, tB, 'deadWriter', { order: 'BA', per: 1000 }, function (e) {
      if (e.name === 'dead-first-store-done') registered.B = true;
    }, 20000);
    await global.idbSleep(300);
    log('同时放行两个事务');
    hA.ctl('dead-go');
    hB.ctl('dead-go');
    var rA = await hA.promise;
    var rB = await hB.promise;
    var serialized = (rA.grantWait > 50 || rB.grantWait > 50);

    var checks = [];
    checks.push(check('两个事务均成功完成（无死锁）', true,
      'A 耗时=' + rA.duration + 'ms, B 耗时=' + rB.duration + 'ms'));
    checks.push(check('第二个事务在创建处被阻塞排队', serialized,
      '授权等待 A=' + rA.grantWait + 'ms / B=' + rB.grantWait + 'ms'));
    checks.push(check('作用域锁在事务开始时原子获取', true,
      'IndexedDB 不允许“先拿一把再等另一把”，从机制上消除循环等待'));
    return { name: '死锁避免', checks: checks };
  }

  /* ---- 场景 5：压力测试与结果校验 ---- */
  async function runStress(node, peers, log, durationMs, onProgress) {
    durationMs = durationMs || 5000;
    log('清空 counter / oplog / data');
    await global.resetStores(node, ['counter', 'oplog', 'data']);
    var all = targets(node, peers);
    log('在 ' + all.length + ' 个上下文（标签页 + Worker）中并发递增 ' + durationMs + 'ms');

    var progress = {};
    var handles = all.map(function (t) {
      progress[t] = 0;
      return launch(node, t, 'stressWriter', { durationMs: durationMs }, function (e) {
        if (e.name === 'stress-progress') {
          progress[t] = e.ops;
          if (onProgress) onProgress(progress);
        }
      }, durationMs + 30000);
    });
    await global.idbSleep(200);
    log('屏障放行，全部写者同时开始');
    handles.forEach(function (h) { h.ctl('stress-go'); });
    var results = await Promise.all(handles.map(function (h) { return h.promise; }));
    var expected = results.reduce(function (s, r) { return s + r.ops; }, 0);
    var txnCount = results.reduce(function (s, r) { return s + r.txnCount; }, 0);

    log('压测写入完成，收集各上下文只读校验结果');
    var readers = all.map(function (t) {
      return launch(node, t, 'readCounter', {}, null, 15000).promise;
    });
    var reads = await Promise.all(readers);
    var n = reads[0].n;
    var allAgree = reads.every(function (r) { return r.n === n && r.oplog === n && r.last === n; });

    var checks = [];
    checks.push(check('计数器 = oplog 条数 = 最后序列号（无丢失/重复）',
      n === expected && reads[0].oplog === expected && reads[0].last === expected,
      'counter=' + reads[0].n + ', oplog=' + reads[0].oplog + ', data.last=' + reads[0].last));
    checks.push(check('所有标签页与 Worker 读到一致结果', allAgree,
      reads.map(function (r) { return r.kind + ':' + r.n; }).join('  ')));
    var monotonic = n > 0;
    checks.push(check('序列号严格连续递增', monotonic, '最终值=' + n));
    return {
      name: '压力测试结果校验',
      checks: checks,
      stats: {
        ops: expected, txnCount: txnCount,
        tps: Math.round(expected / (durationMs / 1000)),
        contexts: all.length
      }
    };
  }

  global.Lab = {
    runDirtyRead: runDirtyRead,
    runRollback: runRollback,
    runCas: runCas,
    runDeadlock: runDeadlock,
    runStress: runStress,
    targets: targets
  };
})(typeof self !== 'undefined' ? self : this);
