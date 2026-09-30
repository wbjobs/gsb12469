/* 页面逻辑：节点管理、日志、校验面板、手动事务、Canvas 时序图 */
(function () {
  'use strict';

  var node = new Node();
  registerActors(node);
  node.announce();

  var workers = [];
  function spawnWorker(label) {
    var w = new Worker('js/worker.js#' + label);
    w.postMessage({ type: 'hello' });
    workers.push(w);
  }
  spawnWorker('workerA');
  spawnWorker('workerB');

  /* ---------- 日志 ---------- */
  var logEl = document.getElementById('log');
  var seqLine = 0;
  function log(msg, level) {
    var d = new Date();
    var hh = pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()) +
      '.' + String(d.getMilliseconds()).padStart(3, '0');
    var line = document.createElement('div');
    line.className = 'log-line ' + (level || 'info');
    var t = document.createElement('span');
    t.className = 't';
    t.textContent = hh + '  ';
    var m = document.createElement('span');
    m.className = 'm';
    m.textContent = msg;
    line.appendChild(t);
    line.appendChild(m);
    logEl.appendChild(line);
    while (logEl.childNodes.length > 400) logEl.removeChild(logEl.firstChild);
    logEl.scrollTop = logEl.scrollHeight;
  }
  function pad(n) { return n < 10 ? '0' + n : '' + n; }

  /* ---------- 节点列表 ---------- */
  var peersEl = document.getElementById('peers');
  function renderPeers(list) {
    var parts = ['<span class="peer-tag tab self">本标签页 (' + node.nodeId.slice(-5) + ')</span>'];
    list.forEach(function (p) {
      var cls = 'peer-tag ' + p.kind;
      var name = p.label + ' (' + p.id.slice(-5) + ')';
      parts.push('<span class="' + cls + '">' + name + '</span>');
    });
    peersEl.innerHTML = parts.join('');
  }
  node.onPeers(renderPeers);

  /* ---------- 校验结果 ---------- */
  var checksEl = document.getElementById('checks');
  function renderResult(result) {
    var html = '<div class="scenario-title">' + result.name + '</div>';
    var allPass = true;
    result.checks.forEach(function (c) {
      if (!c.pass) allPass = false;
      html += '<div class="check ' + (c.pass ? 'pass' : 'fail') + '">' +
        '<div class="top"><span class="badge">' + (c.pass ? '✓' : '✗') + '</span>' +
        '<span>' + c.label + '</span></div>' +
        (c.detail ? '<div class="detail">' + escapeHtml(c.detail) + '</div>' : '') +
        '</div>';
    });
    if (result.stats) {
      var s = result.stats;
      html += '<div class="stats">' + s.contexts + ' 个上下文 · 共 ' + s.ops +
        ' 次增量 / ' + s.txnCount + ' 个写事务 · 约 ' + s.tps + ' ops/s</div>';
    }
    html = '<div class="check ' + (allPass ? 'pass' : 'fail') + '">' +
      '<div class="top"><span class="badge">' + (allPass ? '✓' : '✗') + '</span>' +
      '<span><strong>' + (allPass ? '全部断言通过' : '存在失败断言') +
      '</strong></span></div></div>' + html;
    checksEl.innerHTML = html;
  }
  function escapeHtml(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  /* ---------- 场景运行 ---------- */
  var busy = false;
  var sceneButtons = Array.prototype.slice.call(document.querySelectorAll('[data-scene]'));
  function setBusy(v) {
    busy = v;
    sceneButtons.forEach(function (b) { b.disabled = v; });
  }

  function currentPeers() {
    var list = [];
    var seen = {};
    node.peers && Object.keys(node.peers).forEach(function (id) {
      seen[id] = 1;
      list.push({ id: id, kind: id.indexOf('worker') === 0 ? 'worker' : 'tab' });
    });
    return list;
  }

  async function runScene(name) {
    if (busy) return;
    setBusy(true);
    var peers = currentPeers();
    log('参与节点：本标签页 + ' + peers.length + ' 个远端（' +
      peers.map(function (p) { return p.kind; }).join(', ') + '）');
    try {
      var result = null;
      if (name === 'dirty') result = await Lab.runDirtyRead(node, peers, log);
      if (name === 'rollback') result = await Lab.runRollback(node, peers, log);
      if (name === 'cas') {
        var n = Math.min(Math.max(parseInt(document.getElementById('stressWriters').value, 10) || 8, 1), 32);
        result = await Lab.runCas(node, peers, log, n);
      }
      if (name === 'dead') result = await Lab.runDeadlock(node, peers, log);
      if (name === 'stress') {
        var dur = Math.min(Math.max(parseInt(document.getElementById('stressDuration').value, 10) || 5000, 1000), 20000);
        result = await Lab.runStress(node, peers, log, dur, function () {});
      }
      if (result) {
        renderResult(result);
        var failed = result.checks.filter(function (c) { return !c.pass; }).length;
        log('[' + result.name + '] ' + (failed ? failed + ' 项断言失败' : '全部断言通过'),
          failed ? 'err' : 'ok');
      }
    } catch (err) {
      log('场景执行失败：' + (err && err.message || err), 'err');
      console.error(err);
    } finally {
      setBusy(false);
    }
  }

  sceneButtons.forEach(function (b) {
    b.addEventListener('click', function () { runScene(b.getAttribute('data-scene')); });
  });

  document.getElementById('resetData').addEventListener('click', async function () {
    if (busy) return;
    setBusy(true);
    try {
      await resetStores(node, ['kv', 'data', 'counter', 'oplog']);
      log('已清空全部 object store', 'ok');
    } catch (e) {
      log('清空失败：' + e.message, 'err');
    } finally { setBusy(false); }
  });

  window.addEventListener('beforeunload', function () {
    workers.forEach(function (w) { try { w.terminate(); } catch (e) {} });
  });

  window.__labNode = node;
})();

/* ---------- 手动事务 ---------- */
(function () {
  var node = window.__labNode;
  var manualTx = null;
  var stateEl = document.getElementById('manualState');
  var btns = {
    begin: document.getElementById('manualBegin'),
    get: document.getElementById('manualGet'),
    put: document.getElementById('manualPut'),
    commit: document.getElementById('manualCommit'),
    abort: document.getElementById('manualAbort')
  };
  function setState(s) { stateEl.textContent = s; }
  function setTxBtns(active) {
    btns.get.disabled = !active;
    btns.put.disabled = !active;
    btns.commit.disabled = !active;
    btns.abort.disabled = !active;
  }
  btns.begin.addEventListener('click', async function () {
    var mode = document.getElementById('manualMode').value;
    var stores = mode === 'readwrite' ? ['kv', 'data'] : ['kv', 'data'];
    manualTx = new Txn(node, mode, stores);
    await manualTx.ready;
    setTxBtns(true);
    setState('活动事务 ' + manualTx.id + ' [' + mode + '] —— 在另一标签页/场景中观察它与其他事务的重叠');
    log('手动事务开始 ' + manualTx.id + ' mode=' + mode);
  });
  btns.get.addEventListener('click', async function () {
    if (!manualTx) return;
    var kv = await manualTx.get('kv', 'x');
    var data = await manualTx.get('data', 'x');
    setState('活动事务 ' + manualTx.id + '：kv.x=' + kv + '，data.x=' + data + '（只读事务始终返回快照值）');
  });
  btns.put.addEventListener('click', async function () {
    if (!manualTx || manualTx.mode !== 'readwrite') {
      log('readonly 事务不能写入，请选择 readwrite', 'warn');
      return;
    }
    var v = Math.floor(Math.random() * 100000);
    await manualTx.put('kv', 'x', v);
    await manualTx.put('data', 'x', v);
    setState('活动事务 ' + manualTx.id + '：已写入 ' + v + '，未提交前其他事务不可见');
  });
  btns.commit.addEventListener('click', async function () {
    if (!manualTx) return;
    var t = manualTx;
    manualTx = null;
    await t.done();
    setTxBtns(false);
    setState('事务 ' + t.id + ' 已提交（自动提交发生在事件循环回到空闲时）');
  });
  btns.abort.addEventListener('click', function () {
    if (!manualTx) return;
    var t = manualTx;
    manualTx = null;
    t.abort('manual abort');
    t.done().catch(function () {});
    setTxBtns(false);
    setState('事务 ' + t.id + ' 已中止，所有写入回滚');
  });
})();

/* ---------- Canvas 事务时序图 ---------- */
(function () {
  var node = window.__labNode;
  var canvas = document.getElementById('timeline');
  var ctx = canvas.getContext('2d');
  var WINDOW_MS = 14000;
  var LANE_H = 30;
  var TOP = 8;
  var BAR_H = 15;
  var MARGIN_LEFT = 108;

  var lanes = {};      // from -> { id, label, kind, order }
  var laneOrder = [];
  var txns = {};       // txnId -> { lane, mode, begin, grant, end, phase, stores, conflict }
  var markers = [];    // { t, lane, text, color }
  var lastPrune = 0;

  var COLOR = {
    readonly: '#4f9cff',
    readwrite: '#f0a35e',
    wait: '#8a93a8',
    abort: '#f06a6a',
    marker: '#e7d15e',
    grid: '#222c42',
    text: '#93a0b8'
  };

  function laneFor(ev) {
    var id = ev.from;
    if (!lanes[id]) {
      var kind = id.indexOf('worker') === 0 ? 'worker' : 'tab';
      var label = ev.label
        || (id === node.nodeId ? '本标签页'
          : kind === 'worker' ? 'Worker ' + id.slice(-4) : '标签页 ' + id.slice(-4));
      if (id === node.nodeId) label = '本标签页';
      lanes[id] = { id: id, label: label, kind: kind };
      laneOrder.push(id);
      laneOrder.sort(function (a, b) {
        var ka = lanes[a].kind === 'tab' ? 0 : 1;
        var kb = lanes[b].kind === 'tab' ? 0 : 1;
        if (ka !== kb) return ka - kb;
        return a < b ? -1 : 1;
      });
    }
    return lanes[id];
  }

  node.onTimeline(function (ev) {
    var lane = laneFor(ev);
    if (ev.type === 'txn') {
      var tx = txns[ev.txnId] || (txns[ev.txnId] = {
        lane: lane.id, mode: ev.mode, stores: (ev.stores || []).slice(),
        begin: ev.t, grant: 0, end: 0, phase: 'begin', conflict: false, waitMs: 0
      });
      tx.lane = lane.id;
      if (ev.phase === 'begin') { tx.begin = ev.t; tx.phase = 'begin'; }
      if (ev.phase === 'grant') { tx.grant = ev.t; tx.waitMs = ev.waitMs || 0; tx.phase = 'active'; }
      if (ev.phase === 'commit' || ev.phase === 'abort') {
        tx.end = ev.t;
        tx.phase = ev.phase;
        tx.conflict = !!ev.conflict;
        tx.duration = ev.durationMs;
      }
    } else if (ev.type === 'op') {
      var t = txns[ev.txnId];
      if (t) {
        t.lastOp = ev.t;
        t.lastOpName = ev.op;
      }
    } else if (ev.type === 'note') {
      markers.push({ t: ev.t, lane: lane.id, text: ev.phase, color: COLOR.marker });
      if (markers.length > 200) markers.shift();
    }
  });

  function resizeCanvas() {
    var dpr = window.devicePixelRatio || 1;
    var cssW = canvas.clientWidth || 700;
    var laneCount = Math.max(laneOrder.length, 1);
    var h = TOP * 2 + laneCount * LANE_H + 14;
    canvas.style.height = h + 'px';
    canvas.height = Math.round(h * dpr);
    canvas.width = Math.round(cssW * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  function draw() {
    var nowT = Date.now();
    var t0 = nowT - WINDOW_MS;
    var laneCount = Math.max(laneOrder.length, 1);
    resizeCanvas();
    var cssW = canvas.clientWidth || 700;
    var plotW = cssW - MARGIN_LEFT - 14;

    ctx.clearRect(0, 0, cssW, canvas.height);

    // 网格：每 2 秒一条
    ctx.strokeStyle = COLOR.grid;
    ctx.lineWidth = 1;
    ctx.font = '10px ui-monospace, Menlo, monospace';
    ctx.fillStyle = COLOR.text;
    var stepMs = 2000;
    var firstGrid = Math.ceil(t0 / stepMs) * stepMs;
    for (var gt = firstGrid; gt <= nowT; gt += stepMs) {
      var xg = MARGIN_LEFT + (gt - t0) / WINDOW_MS * plotW;
      ctx.beginPath();
      ctx.moveTo(xg, TOP - 2);
      ctx.lineTo(xg, TOP + laneCount * LANE_H + 4);
      ctx.stroke();
      var age = Math.round((nowT - gt) / 1000);
      ctx.fillText('-' + age + 's', xg - 12, TOP - 1);
    }

    // 泳道
    laneOrder.forEach(function (id, idx) {
      var lane = lanes[id];
      var y = TOP + idx * LANE_H;
      ctx.fillStyle = COLOR.text;
      ctx.font = '11px sans-serif';
      ctx.fillText(lane.label, 8, y + BAR_H + 1);
      ctx.strokeStyle = COLOR.grid;
      ctx.beginPath();
      ctx.moveTo(MARGIN_LEFT - 8, y + LANE_H - 6);
      ctx.lineTo(cssW - 10, y + LANE_H - 6);
      ctx.stroke();
    });

    function xOf(t) { return MARGIN_LEFT + Math.max(0, t - t0) / WINDOW_MS * plotW; }

    // 事务条
    Object.keys(txns).forEach(function (tid) {
      var tx = txns[tid];
      var idx = laneOrder.indexOf(tx.lane);
      if (idx === -1) return;
      var y = TOP + idx * LANE_H;
      var b = Math.max(tx.begin, t0);
      var e = tx.end || nowT;
      if (e < t0) return;
      var x1 = xOf(b);
      var x2 = xOf(Math.min(e, nowT));
      var base = tx.mode === 'readonly' ? COLOR.readonly : COLOR.readwrite;

      // 等待授权段
      var gEnd = tx.grant || (tx.phase === 'begin' ? nowT : tx.end || nowT);
      if (gEnd > b) {
        ctx.fillStyle = COLOR.wait;
        var gx2 = xOf(Math.min(gEnd, nowT));
        ctx.fillRect(x1, y, Math.max(2, gx2 - x1), BAR_H);
        x1 = Math.max(x1, gx2);
      }
      // 活动段
      var activeStart = tx.grant || tx.begin;
      var ax1 = xOf(Math.max(activeStart, t0));
      ctx.fillStyle = tx.phase === 'abort' ? COLOR.abort : base;
      ctx.fillRect(Math.min(ax1, x2), y, Math.max(2, x2 - Math.min(ax1, x2)), BAR_H);

      // 结束标记
      if (tx.phase === 'commit' || tx.phase === 'abort') {
        ctx.fillStyle = tx.phase === 'abort' ? COLOR.abort : '#4ec98f';
        ctx.fillRect(x2 - 1.5, y - 2, 3, BAR_H + 4);
        if (tx.conflict) {
          ctx.fillStyle = COLOR.abort;
          ctx.font = '9px ui-monospace, monospace';
          ctx.fillText('冲突', x2 + 3, y + BAR_H - 2);
        }
      } else {
        // 活动中：右端脉冲
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(x2 - 1, y - 1, 2, BAR_H + 2);
      }
    });

    // 事件标记
    markers.forEach(function (mk) {
      if (mk.t < t0 || mk.t > nowT) return;
      var idx = laneOrder.indexOf(mk.lane);
      if (idx === -1) return;
      var x = xOf(mk.t);
      var y = TOP + idx * LANE_H + BAR_H + 13;
      ctx.fillStyle = mk.color;
      ctx.beginPath();
      ctx.arc(x, y - 9, 2, 0, Math.PI * 2);
      ctx.fill();
    });

    // 清理过期事务
    if (nowT - lastPrune > 1000) {
      lastPrune = nowT;
      Object.keys(txns).forEach(function (tid) {
        var tx = txns[tid];
        if (tx.end && nowT - tx.end > WINDOW_MS + 2000) delete txns[tid];
      });
      markers = markers.filter(function (mk) { return nowT - mk.t < WINDOW_MS; });
    }

    requestAnimationFrame(draw);
  }
  requestAnimationFrame(draw);
})();
