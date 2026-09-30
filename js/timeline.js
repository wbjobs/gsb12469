// Canvas 事务时序图：横轴时间，纵轴事务来源（标签页 / Worker）。
const COLORS = {
  readonly: '#4aa3ff',
  readwrite: '#f0a13c',
  commit: '#5fd68a',
  abort: '#ff7b7b',
  conflict: '#d98cff',
  grid: '#1e2a45',
  text: '#8fa3bf',
};

export function createTimeline(canvas) {
  const ctx = canvas.getContext('2d');
  const transactions = new Map(); // txId -> {source, mode, start, end, endType, marks:[]}

  function addEvent(event) {
    if (!event.txId) return; // info 等非事务事件不上时序图
    // 统一用 wall 时间（Date.now 由发送方附带），保证跨标签页可比
    const ts = event.wallTs != null ? event.wallTs : Date.now();
    let tx = transactions.get(event.txId);
    if (!tx) {
      tx = { source: event.source, mode: event.mode, start: ts, end: null, endType: null, marks: [] };
      transactions.set(event.txId, tx);
    }
    tx.start = Math.min(tx.start, ts);
    if (event.type === 'commit' || event.type === 'abort') {
      tx.end = ts;
      tx.endType = event.type;
    } else if (event.type === 'conflict') {
      tx.marks.push({ ts, kind: 'conflict' });
    }
    // 限制内存
    if (transactions.size > 600) {
      const oldest = transactions.keys().next().value;
      transactions.delete(oldest);
    }
  }

  function clear() {
    transactions.clear();
    draw();
  }

  function draw({ followTail = true } = {}) {
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth;
    const h = canvas.clientHeight || 260;
    if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
      canvas.width = w * dpr;
      canvas.height = h * dpr;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = '#0b101d';
    ctx.fillRect(0, 0, w, h);

    const txs = [...transactions.values()].filter((t) => t.start != null);
    if (txs.length === 0) {
      ctx.fillStyle = COLORS.text;
      ctx.font = '13px sans-serif';
      ctx.fillText('暂无事务，执行一次事务或压力测试后此处显示时序。', 16, 30);
      return;
    }

    const now = Date.now();
    let minTs = Infinity;
    let maxTs = -Infinity;
    for (const t of txs) {
      minTs = Math.min(minTs, t.start);
      maxTs = Math.max(maxTs, t.end || now);
    }
    if (followTail) {
      const span = Math.max(maxTs - minTs, 2000);
      minTs = now - span;
    }
    if (maxTs - minTs < 50) maxTs = minTs + 50;

    const sources = [...new Set(txs.map((t) => t.source))].sort();
    const leftPad = 130;
    const topPad = 24;
    const rowH = Math.min(34, (h - topPad - 18) / Math.max(sources.length, 1));
    const x = (ts) => leftPad + ((ts - minTs) / (maxTs - minTs)) * (w - leftPad - 12);

    // 网格 + 时间刻度
    ctx.strokeStyle = COLORS.grid;
    ctx.fillStyle = COLORS.text;
    ctx.font = '10px sans-serif';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 5; i++) {
      const ts = minTs + ((maxTs - minTs) * i) / 5;
      const gx = x(ts);
      ctx.beginPath();
      ctx.moveTo(gx, topPad - 14);
      ctx.lineTo(gx, h - 14);
      ctx.stroke();
      ctx.fillText(`+${((ts - minTs) / 1000).toFixed(1)}s`, gx - 12, topPad - 4);
    }

    // 行标签
    ctx.font = '11px sans-serif';
    sources.forEach((src, i) => {
      const y = topPad + i * rowH + rowH / 2;
      ctx.fillStyle = COLORS.text;
      ctx.fillText(src.slice(0, 20), 8, y + 4);
      ctx.strokeStyle = COLORS.grid;
      ctx.beginPath();
      ctx.moveTo(leftPad - 4, y);
      ctx.lineTo(w - 12, y);
      ctx.stroke();
    });

    // 事务条
    for (const t of txs) {
      const row = sources.indexOf(t.source);
      if (row < 0) continue;
      const y = topPad + row * rowH + rowH / 2;
      const x1 = x(t.start);
      const x2 = x(t.end != null ? t.end : now);
      const color = COLORS[t.mode] || COLORS.readwrite;
      ctx.fillStyle = color + '55';
      ctx.strokeStyle = color;
      const bh = Math.min(12, rowH - 6);
      ctx.fillRect(x1, y - bh / 2, Math.max(x2 - x1, 2), bh);
      ctx.strokeRect(x1, y - bh / 2, Math.max(x2 - x1, 2), bh);

      if (t.endType) {
        ctx.fillStyle = COLORS[t.endType];
        if (t.endType === 'commit') {
          ctx.beginPath();
          ctx.arc(x2, y, 4, 0, Math.PI * 2);
          ctx.fill();
        } else {
          ctx.font = 'bold 11px sans-serif';
          ctx.fillText('✖', x2 - 4, y + 4);
        }
      }
      for (const m of t.marks) {
        if (m.kind === 'conflict') {
          ctx.fillStyle = COLORS.conflict;
          const mx = x(m.ts);
          ctx.beginPath();
          ctx.moveTo(mx, y - 8);
          ctx.lineTo(mx + 5, y);
          ctx.lineTo(mx, y + 8);
          ctx.lineTo(mx - 5, y);
          ctx.closePath();
          ctx.fill();
        }
      }
    }
  }

  return { addEvent, clear, draw };
}
