# IndexedDB 并发实验台

多标签页 + 多 Web Worker 对同一 IndexedDB 并发读写，观察事务隔离与并发行为。纯原生技术栈（IndexedDB / Web Worker / BroadcastChannel / Canvas），无任何框架。

## 运行

ES module 与 module Worker 不能用 `file://` 打开，需要静态服务器：

```bash
cd 本目录
python3 -m http.server 8000
# 打开 http://localhost:8000 ，建议多开几个标签页
```

## 功能

- **单次事务**：选择 readonly / readwrite、操作类型（读、读-改-写、写入后回滚、慢速读-改-写）、在当前标签页或 Worker 中执行。
- **压力测试**：N 个执行者（主线程 + Worker 池）并发递增共享计数器，结束后自动校验最终值是否等于期望总数。`atomic`（单事务读-改-写）必然通过；`naive`（读写分两个事务）演示丢失更新。
- **隔离性验证**（一键场景）：
  - 回滚不影响其他事务（abort 后最终值只包含已提交写入）
  - 乐观锁冲突可检测（version 字段跨事务校验，冲突方自动放弃）
  - 交叉读写不死锁（IDB 对同作用域 readwrite 事务串行化，机制上无死锁）
  - 并发写不脏读（写入延迟期间并发只读，读序列只能看到旧值或新值）
- **时序图**：Canvas 绘制所有标签页与 Worker 的事务条（开始→提交/回滚），冲突以菱形标记。
- **跨标签页汇总**：事件经 BroadcastChannel 广播，任一标签页都能看到全局事务视图。

## 关键机制说明

- IndexedDB 对作用域重叠的 readwrite 事务**串行执行**，因此单事务内的读-改-写天然原子，不会脏读、不会丢失更新。
- 冲突检测需要在**跨事务**边界上做（乐观锁 version 校验），因为单事务内 IDB 已经帮你串行化了。
- IDB 没有显式锁，事务自动提交，不存在锁等待图，因此不会死锁。
- 事务事件时间戳使用 `Date.now()`（墙钟），保证跨标签页、跨 Worker 可比较。

## 文件结构

- `index.html` — 页面与控制面板
- `js/db.js` — 带插桩的 IndexedDB 封装（主线程 / Worker 共用）
- `js/worker.js` — Worker 线程执行事务
- `js/bus.js` — BroadcastChannel 事件总线
- `js/stress.js` — 压力测试与四个隔离性验证场景
- `js/timeline.js` — Canvas 事务时序图
- `js/main.js` — UI 装配与事件转发
