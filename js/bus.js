// BroadcastChannel 事件总线：汇总本标签页、Worker、其他标签页的事务事件。
export const CHANNEL = 'idb-concurrency-lab-bus';

export function createBus(tabId, onEvent) {
  const channel = new BroadcastChannel(CHANNEL);
  const peers = new Map();
  peers.set(tabId, Date.now());

  channel.onmessage = (e) => {
    const msg = e.data;
    if (!msg || msg.from === tabId) return;
    if (msg.kind === 'event') {
      onEvent(msg.event);
    } else if (msg.kind === 'hello' || msg.kind === 'heartbeat') {
      peers.set(msg.from, Date.now());
      if (msg.kind === 'hello') {
        channel.postMessage({ kind: 'heartbeat', from: tabId });
      }
    }
  };

  channel.postMessage({ kind: 'hello', from: tabId });

  return {
    post(event) {
      channel.postMessage({ kind: 'event', from: tabId, event });
    },
    peerCount() {
      const now = Date.now();
      for (const [id, ts] of peers) {
        if (now - ts > 15000) peers.delete(id);
      }
      return peers.size;
    },
    close() { channel.close(); },
  };
}
