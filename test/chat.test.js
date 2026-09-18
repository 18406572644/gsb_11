'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 启动一个隔离的测试服务器（内存库、随机端口、默认关闭重发以免干扰计数） */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000, // 默认不在测试内重发；重发场景单独配置
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

/** 测试客户端：手动 ACK（测试可控）。log 全量记录供断言；waitFor 消费式匹配（每帧至多满足一个等待者） */
class Client {
  static async connect(port, token, { autoAck = false } = {}) {
    const c = new Client();
    c.autoAck = autoAck; // 自动累积 ACK（模拟正常消费的快设备）
    c.closeCode = null;
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      if (autoAck && m.type === 'msg') c.send({ type: 'ack', roomId: m.roomId, seq: m.seq });
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', (code) => { c.closeCode = code; c._onClosed(); });
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        reject(new Error('waitFor: timed out'));
      }, timeout);
      this.waiters.push(w);
    });
  }

  /** 已收到的某房间消息帧（seq 列表） */
  roomSeqs(roomId) {
    return this.log.filter((m) => m.type === 'msg' && m.roomId === roomId).map((m) => m.seq);
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  return client.waitFor((m) => m.type === 'joined');
}

// ---------------------------------------------------------------- 测试用例

test('登录、连接、建房后成为管理员', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    assert.ok(u.userId && u.token);
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    const joined = a.log.find((m) => m.type === 'joined');
    assert.equal(joined.role, 'admin');
    assert.ok(roomId);
    await a.close();
  } finally {
    server.stop();
  }
});

test('发送收到 ACK，房间内广播按 seq 全序投递', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    for (let i = 1; i <= 3; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `hello ${i}` });
    }
    // 发送者收到 3 个 ACK，seq 递增
    for (let i = 1; i <= 3; i++) {
      const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${i}`);
      assert.equal(ack.seq, i);
    }
    // 接收者按序收到 1,2,3
    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 3);
    assert.deepEqual(b.roomSeqs(roomId), [1, 2, 3]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复 clientMsgId 幂等：返回同一 seq，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup-1');
    // 网络重试：同 clientMsgId 重发
    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack2 = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'dup-1' && m !== ack1
    );
    assert.equal(ack1.seq, ack2.seq);

    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId);
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [1], '接收端只应收到一次广播');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发：重连后按序补齐离线期间的消息，且不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'online' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close(); // —— B 掉线 ——

    for (const [i, c] of [2, 3, 4].entries()) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i + 2}`, content: `offline ${c}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm4');

    // —— B 重连，携带本地进度 lastSeq=1 ——
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2, 3, 4], '补发且仅补发缺口，按序到达');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('已追平的连接重连后不再收到旧消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1); // 已追平
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [], '不应有任何补发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('服务端对未 ACK 消息重发，ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    // 不 ACK，等服务端重发
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1, 2000);
    assert.ok(b.roomSeqs(roomId).length >= 2, '应观察到至少一次重发');

    b.send({ type: 'ack', roomId, seq: 1 });
    await sleep(100);
    const countAfterAck = b.roomSeqs(roomId).length;
    await sleep(400);
    assert.equal(b.roomSeqs(roomId).length, countAfterAck, 'ACK 后不应再有重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言：管理员可禁言/解禁，被禁言者发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted' && m.userId === ub.userId);

    b.send({ type: 'msg', roomId, clientMsgId: 'x1', content: 'am i muted?' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'unmuted');

    b.send({ type: 'msg', roomId, clientMsgId: 'x2', content: 'free again' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'x2');
    assert.equal(ack.seq, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：普通成员不能禁言他人，管理员不可被禁言', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    b.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    const err1 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    a.send({ type: 'mute', roomId, userId: ua.userId, minutes: 5 });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('连接数限制：单用户连接数超限被拒绝', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 2 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const c2 = await Client.connect(port, u.token);
    await assert.rejects(
      Client.connect(port, u.token),
      /503|TOO_MANY_DEVICES|Unexpected server response/
    );
    await c1.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('发送限流：突发超过令牌桶被拒绝', async () => {
  const { server, port } = await startServer({ rateLimitPerSec: 1, rateLimitBurst: 2 });
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');

    for (let i = 0; i < 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `r${i}`, content: `spam ${i}` });
    }
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'RATE_LIMITED');
    assert.ok(err);
    await sleep(300);
    const ackCount = a.log.filter((m) => m.type === 'ack').length;
    assert.equal(ackCount, 2, '突发容量为 2，其余应被限流');
    await a.close();
  } finally {
    server.stop();
  }
});

test('历史消息分页拉取', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    for (let i = 1; i <= 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `h${i}`, content: `msg ${i}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'h5');

    a.send({ type: 'history', roomId, beforeSeq: 4, limit: 2 });
    const h = await a.waitFor((m) => m.type === 'history');
    assert.deepEqual(h.messages.map((m) => m.seq), [2, 3], '升序返回 beforeSeq 之前的一页');
    assert.equal(h.hasMore, true);
    await a.close();
  } finally {
    server.stop();
  }
});

test('服务端游标兜底：新设备不带 lastSeq 时从已确认进度继续', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'first' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    b.send({ type: 'ack', roomId, seq: 1 }); // 上报确认进度
    await sleep(100);
    await b.close();

    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'second' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm2');

    // 新设备重连，不带 lastSeq —— 应使用服务端游标，只补 seq 2
    b = await Client.connect(port, ub.token);
    b.send({ type: 'join', room: roomId });
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('持久化：服务重启后消息不丢失', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      for (let i = 1; i <= 3; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `p${i}`, content: `durable ${i}` });
      }
      await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token); // 同一 token 仍有效
      await joinRoom(a, roomId, 0);
      await a.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
      assert.deepEqual(a.roomSeqs(roomId), [1, 2, 3], '重启后历史消息完整可补发');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

async function sendBurst(a, roomId, n, start = 1) {
  for (let i = start; i < start + n; i++) {
    a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `msg ${i}` });
  }
  await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${start + n - 1}`);
}

test('背压：慢设备被暂停且积压被钉死，同账号快设备持续正常消费', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 10,
    backpressureResumeRatio: 0.5,
    backpressureDisconnectMs: 60_000, // 本用例不断开，只验证暂停
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token, { autoAck: true }); // 发送者正常 ACK 自己的广播回包
    const slow = await Client.connect(port, ub.token); // 慢设备：不 ACK
    const fast = await Client.connect(port, ub.token, { autoAck: true }); // 同账号快设备
    const roomId = await createRoom(a, 'general');
    await joinRoom(slow, roomId);
    await joinRoom(fast, roomId);

    await sendBurst(a, roomId, 10); // 第一批恰好到软上限：快设备 ACK 排空，慢设备积压 10
    await sleep(100);
    await sendBurst(a, roomId, 5, 11); // 第二批：快设备照常收，慢设备在第 11 条处暂停
    await fast.waitFor((m) => m.type === 'msg' && m.seq === 15);

    const paused = await slow.waitFor((m) => m.type === 'backpressure' && m.state === 'paused');
    assert.equal(paused.limit, 10);
    assert.equal(paused.unacked, 10);

    // 慢设备：只收到 1..10，11..15 在暂停期间被丢弃（不缓冲），积压钉死在上限
    assert.deepEqual(slow.roomSeqs(roomId), Array.from({ length: 10 }, (_, i) => i + 1));
    await sleep(200);
    assert.deepEqual(slow.roomSeqs(roomId).length, 10, '暂停期间不得继续向慢连接推送');

    const bobConns = [...server.hub.all].filter((c) => c.userId === ub.userId);
    const slowConn = bobConns.find((c) => c.paused);
    const fastConn = bobConns.find((c) => !c.paused);
    assert.ok(slowConn, '慢连接处于暂停态');
    assert.equal(slowConn.unackedCount, 10, '慢连接未确认积压被钉死在上限');
    assert.ok(fastConn, '同账号快连接不受影响');
    assert.equal(fastConn.unackedCount, 0, '快连接 ACK 正常，无积压');

    // 快设备 15 条全部实时消费，连接保持健康
    assert.deepEqual(fast.roomSeqs(roomId), Array.from({ length: 15 }, (_, i) => i + 1));
    assert.equal(fast.ws.readyState, 1);
    assert.equal(server.hub.stats().paused, 1, '全房间只有慢连接一台处于暂停态');

    await slow.close();
    await fast.close();
    await a.close();
  } finally {
    server.stop();
  }
});

test('背压恢复：ACK 排空到恢复水位后 resume，客户端 sync 补齐缺口', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 10,
    backpressureResumeRatio: 0.5, // 恢复水位 5
    backpressureDisconnectMs: 60_000,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    await sendBurst(a, roomId, 12); // b 在第 11 条处暂停，只收到 1..10
    await b.waitFor((m) => m.type === 'backpressure' && m.state === 'paused');

    // 累积 ACK 到 seq 5：积压 10 -> 5，触达恢复水位
    b.send({ type: 'ack', roomId, seq: 5 });
    const resume = await b.waitFor((m) => m.type === 'backpressure' && m.state === 'resume');
    assert.deepEqual(resume.rooms, [roomId], 'resume 须指明需要重同步的房间');

    // 客户端按自己的本地进度（最后见到 seq 10）补洞，而非用服务端用户游标
    b.send({ type: 'sync', roomId, lastSeq: 10 });
    const done = await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.equal(done.hasMore, false);
    assert.deepEqual(b.roomSeqs(roomId), Array.from({ length: 12 }, (_, i) => i + 1), '缺口 11、12 补齐');

    b.send({ type: 'ack', roomId, seq: 12 });
    await sleep(50);
    const conn = [...server.hub.all].find((c) => c.userId === ub.userId);
    assert.equal(conn.paused, false);
    assert.equal(conn.unackedCount, 0);
    assert.equal(server.db.getCursor(roomId, ub.userId), 12, '游标机制正常推进');

    await b.close();
    await a.close();
  } finally {
    server.stop();
  }
});

test('背压断开：宽限期满只关慢连接（1013），其他设备、房间与游标均不受影响', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 5,
    backpressureDisconnectMs: 200,
    backpressureSweepMs: 50,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token, { autoAck: true });
    const slow = await Client.connect(port, ub.token); // 慢设备
    const fast = await Client.connect(port, ub.token, { autoAck: true }); // 同账号快设备
    const roomId = await createRoom(a, 'general');
    await joinRoom(slow, roomId);
    await joinRoom(fast, roomId);

    await sendBurst(a, roomId, 5); // 第一批到软上限：快设备排空，慢设备积压 5
    await sleep(100);
    await sendBurst(a, roomId, 5, 6); // 第二批：慢设备在第 6 条处暂停
    await fast.waitFor((m) => m.type === 'msg' && m.seq === 10);

    const closeCode = await Promise.race([
      slow.closed.then(() => slow.closeCode),
      sleep(3000).then(() => null),
    ]);
    assert.equal(closeCode, 1013, '持续不排空的慢连接应被 1013 关闭');

    // 同账号另一设备、发送者都保持在线
    assert.equal(fast.ws.readyState, 1);
    assert.equal(a.ws.readyState, 1);
    assert.deepEqual(fast.roomSeqs(roomId), Array.from({ length: 10 }, (_, i) => i + 1));

    // hub 中 bob 只剩快连接（等服务端处理完 close 事件）；游标停留在快设备 ACK
    // 到的位置，断开未破坏游标
    let bobConns;
    for (let i = 0; i < 50; i++) {
      bobConns = [...server.hub.all].filter((c) => c.userId === ub.userId);
      if (bobConns.length === 1) break;
      await sleep(20);
    }
    assert.equal(bobConns.length, 1, '只移除慢连接，同账号其他设备保留');
    assert.equal(bobConns[0].paused, false);
    assert.equal(server.db.getCursor(roomId, ub.userId), 10);

    // 慢设备重连：重新 join 后按本地进度 sync 补齐缺口
    const reconnected = await Client.connect(port, ub.token);
    await joinRoom(reconnected, roomId, 5);
    await reconnected.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(reconnected.roomSeqs(roomId), [6, 7, 8, 9, 10], '重连补发完整');
    assert.equal(server.db.getCursor(roomId, ub.userId), 10, '补发不回退游标');

    await reconnected.close();
    await fast.close();
    await a.close();
  } finally {
    server.stop();
  }
});

test('背压截断补发：回放途中触发暂停时 sync_done 如实上报缺口', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 10,
    backpressureResumeRatio: 0.5,
    backpressureDisconnectMs: 60_000,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'general');
    await sendBurst(a, roomId, 12); // 12 条历史消息

    // 慢设备带着 lastSeq=0 入房：回放 1..10 后在第 11 条触发暂停并截断
    const b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 0);
    const done = await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.equal(done.lastSeq, 10, '只如实上报实际投递到的位置');
    assert.equal(done.hasMore, true, '未投部分必须报有缺口，由客户端续拉');
    assert.deepEqual(b.roomSeqs(roomId), Array.from({ length: 10 }, (_, i) => i + 1));

    await b.close();
    await a.close();
  } finally {
    server.stop();
  }
});

