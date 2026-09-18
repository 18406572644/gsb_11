'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Hub, Connection } = require('../src/hub');
const { now } = require('../src/util');

/** 假 WebSocket：记录发送、close/terminate，可手动操控 bufferedAmount */
function fakeWs() {
  return {
    readyState: 1,
    bufferedAmount: 0,
    sent: [],
    closeCode: null,
    terminated: false,
    send(str) { this.sent.push(str); },
    close(code, reason) { this.closeCode = code; this.closeReason = reason; this.readyState = 3; },
    terminate() { this.terminated = true; this.readyState = 3; },
    ping() {},
  };
}

function makeHub(overrides = {}) {
  return new Hub({
    maxConnections: 100,
    maxConnectionsPerUser: 3,
    maxUnackedPerConn: 10,
    maxSendBufferBytes: 1000,
    backpressureResumeRatio: 0.5,
    backpressureTimeoutMs: 500,
    heartbeatTimeoutMs: 999_999,
    ackResendAfterMs: 999_999,
    ackMaxResend: 5,
    ...overrides,
  });
}

const alice = () => ({ id: 'u_alice', name: 'alice' });
const bob = () => ({ id: 'u_bob', name: 'bob' });

function openConn(hub, user) {
  const conn = new Connection(fakeWs(), user);
  hub.add(conn);
  return conn;
}

const msg = (seq) => ({ type: 'msg', seq });

test('未 ACK 达高水位：该连接暂停、实时帧跳过且只标记缺口房间，控制帧照发', () => {
  const hub = makeHub();
  const conn = openConn(hub, alice());
  hub.joinRoom(conn, 'r1');

  for (let seq = 1; seq <= 10; seq++) {
    assert.equal(hub.send(conn, msg(seq), { track: true, roomId: 'r1', seq }), true);
  }
  assert.equal(conn.unackedCount, 10);
  assert.equal(conn.paused, false);

  // 第 11 条触发高水位：不发送、不入未 ACK 队列（内存有界），仅记录缺口
  assert.equal(hub.send(conn, msg(11), { track: true, roomId: 'r1', seq: 11 }), false);
  assert.equal(conn.paused, true);
  assert.equal(conn.pauseReason, 'unacked');
  assert.equal(conn.unackedCount, 10, '暂停后积压不得继续增长');
  assert.deepEqual([...conn.blockedRooms], ['r1']);
  assert.equal(conn.ws.sent.length, 10);

  // 暂停期间控制帧（不 track）仍然下发
  assert.equal(hub.send(conn, { type: 'sync_done', roomId: 'r1', lastSeq: 10 }), true);
});

test('ACK 排水到低水位才恢复（滞回），恢复时按缺口房间回调 onDrain', () => {
  const hub = makeHub();
  const drained = [];
  hub.onDrain = (conn, rooms) => drained.push({ conn, rooms });

  const conn = openConn(hub, alice());
  hub.joinRoom(conn, 'r1');
  for (let seq = 1; seq <= 11; seq++) {
    hub.send(conn, msg(seq), { track: true, roomId: 'r1', seq });
  }
  assert.equal(conn.paused, true);

  // ACK 到 seq=5：unacked=5，恰等于高水位的一半；低水位要求严格小于 5，不恢复
  conn.ack('r1', 5);
  hub.tryResume(conn);
  assert.equal(conn.paused, true);
  assert.equal(drained.length, 0);

  // 继续排水到 4 条以下（ACK 到 6 → unacked=4 < 5）→ 恢复并回放
  conn.ack('r1', 6);
  hub.tryResume(conn);
  assert.equal(conn.paused, false);
  assert.equal(drained.length, 1);
  assert.deepEqual(drained[0].rooms, ['r1']);
  assert.equal(drained[0].conn, conn);
  assert.equal(conn.blockedRooms.size, 0);
});

test('发送缓冲区高水位同样暂停，缓冲区回落到低水位后由扫描恢复', () => {
  const hub = makeHub();
  const drained = [];
  hub.onDrain = (conn, rooms) => drained.push(rooms);
  const conn = openConn(hub, bob());
  hub.joinRoom(conn, 'r1');

  // 客户端读得慢：TCP 发送缓冲区堆满，哪怕一条都没 ACK 失败也应暂停
  conn.ws.bufferedAmount = 1000;
  hub.backpressureSweep();
  assert.equal(conn.paused, true);
  assert.equal(conn.pauseReason, 'buffer');

  // 排水到 600：高于低水位 500，滞回区间内不恢复
  conn.ws.bufferedAmount = 600;
  hub.backpressureSweep();
  assert.equal(conn.paused, true);
  assert.equal(drained.length, 0);

  // 回落到 499 < 500：恢复
  conn.ws.bufferedAmount = 499;
  hub.backpressureSweep();
  assert.equal(conn.paused, false);
  assert.equal(drained.length, 0, '没有跳过的消息（无缺口房间）时无需回放回调');
});

test('暂停超宽限：仅 terminate 该慢连接，同一用户的其他设备连接不受影响', () => {
  const hub = makeHub();
  const slow = openConn(hub, alice()); // 设备 1（慢）
  const fast = openConn(hub, alice()); // 设备 2（正常）
  hub.joinRoom(slow, 'r1');
  hub.joinRoom(fast, 'r1');

  for (let seq = 1; seq <= 11; seq++) {
    hub.send(slow, msg(seq), { track: true, roomId: 'r1', seq });
  }
  assert.equal(slow.paused, true);

  // 快设备全程正常
  assert.equal(hub.send(fast, msg(11), { track: true, roomId: 'r1', seq: 11 }), true);
  assert.equal(fast.paused, false);

  // 时间快进到宽限期之后
  slow.pausedSince = now() - 1000;
  hub.backpressureSweep();

  assert.equal(slow.ws.terminated, true, '慢连接应被 terminate');
  assert.equal(fast.ws.terminated, false, '同用户快连接不得受牵连');
  assert.equal(fast.ws.closeCode, null);

  // 模拟 slow 的 close 事件清理：byUser 中只摘除 slow，fast 完好
  hub.remove(slow);
  assert.equal(hub.all.has(slow), false);
  assert.equal(hub.all.has(fast), true);
  assert.deepEqual([...hub.byUser.get('u_alice')], [fast]);
  assert.equal(hub.send(fast, { type: 'notice' }), true, '快连接继续可用');
});

test('广播隔离：慢连接跳过的消息仍投递给同房间其他连接', () => {
  const hub = makeHub();
  const slow = openConn(hub, alice());
  const other = openConn(hub, bob());
  hub.joinRoom(slow, 'r1');
  hub.joinRoom(other, 'r1');
  for (let seq = 1; seq <= 11; seq++) {
    hub.send(slow, msg(seq), { track: true, roomId: 'r1', seq });
  }
  assert.equal(slow.paused, true);

  const delivered = hub.broadcast('r1', msg(42), { track: true, seq: 42 });
  assert.equal(delivered, 1, '仅 other 收到');
  assert.equal(other.unackedCount, 1);
  assert.equal(slow.unackedCount, 10, '慢连接不增加积压');
  assert.ok(slow.blockedRooms.has('r1'));

  // 控制帧广播两者都收到
  assert.equal(hub.broadcast('r1', { type: 'notice' }), 2);
});

test('trackUnacked 对同一 seq 幂等：重复追踪不重复计数', () => {
  const hub = makeHub();
  const conn = openConn(hub, alice());
  hub.joinRoom(conn, 'r1');
  hub.send(conn, msg(7), { track: true, roomId: 'r1', seq: 7 });
  conn.trackUnacked('r1', 7, 'again'); // 重放/实时边界重叠
  assert.equal(conn.unackedCount, 1);
  assert.equal(conn.unacked.get('r1').get(7).frame, 'again');
});

test('resendSweep：暂停连接不叠加重发，恢复后才补发', () => {
  const hub = makeHub({ ackResendAfterMs: 100 });
  const conn = openConn(hub, alice());
  hub.joinRoom(conn, 'r1');
  hub.send(conn, msg(1), { track: true, roomId: 'r1', seq: 1 });
  const sentAfterFirst = conn.ws.sent.length;

  // 制造一条超时未 ACK 条目，并令连接因缓冲区暂停
  conn.unacked.get('r1').get(1).lastSent = 0;
  conn.ws.bufferedAmount = 1000;
  hub.backpressureSweep();
  assert.equal(conn.paused, true);

  hub.resendSweep();
  assert.equal(conn.ws.sent.length, sentAfterFirst, '暂停期间不得向堆满的缓冲区叠加重发');

  // 排水恢复后，下一轮扫描正常重发
  conn.ws.bufferedAmount = 0;
  hub.tryResume(conn);
  hub.resendSweep();
  assert.equal(conn.ws.sent.length, sentAfterFirst + 1);
});

test('离开房间清理该连接在该房间的背压状态，不影响其他房间', () => {
  const hub = makeHub();
  const conn = openConn(hub, alice());
  hub.joinRoom(conn, 'r1');
  hub.joinRoom(conn, 'r2');
  hub.send(conn, msg(1), { track: true, roomId: 'r1', seq: 1 });
  conn.blockedRooms.add('r1');
  hub.leaveRoom(conn, 'r1');
  assert.equal(conn.unacked.has('r1'), false);
  assert.equal(conn.blockedRooms.has('r1'), false);
  assert.equal(conn.roomProgress.has('r1'), false);
  assert.ok(conn.rooms.has('r2'));
});
