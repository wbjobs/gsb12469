/* Web Worker 入口：与标签页共享同一个 IndexedDB 与 BroadcastChannel 总线 */
'use strict';
try {
  importScripts(new URL('common.js', self.location).href);
} catch (e) {
  console.error('worker 加载 common.js 失败', e);
}

var node = new self.Node();
self.registerActors(node);
node.announce();

self.addEventListener('message', function (ev) {
  var msg = ev.data || {};
  if (msg.type === 'shutdown') self.close();
  if (msg.type === 'hello') {
    node.announce();
    self.postMessage({ type: 'ready', nodeId: node.nodeId });
  }
});

self.addEventListener('offline', function () {});
self.postMessage({ type: 'ready', nodeId: node.nodeId });
