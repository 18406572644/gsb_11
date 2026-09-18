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
  static async connect(port, token) {
    const c = new Client();
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
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
    c.ws.on('close', () => c._onClosed());
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

const withTimeout = (p, ms, label) =>
  Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout: ${label}`)), ms))]);

/** 服务端侧：某用户的全部连接对象（用于断言连接级背压状态） */
function serverConns(server, userId) {
  return [...server.hub.all].filter((c) => c.userId === userId).sort((a, b) => a.id - b.id);
}

/**
 * 让客户端对某房间的每条 msg 自动累积 ACK（模拟消费正常的快设备）。
 * 直接挂在底层 ws 上，与 Client.waitFor 的消费互不干扰。返回停止函数。
 */
function autoAck(client, roomId) {
  const handler = (raw) => {
    const m = JSON.parse(raw.toString());
    if (m.type === 'msg' && m.roomId === roomId) {
      client.send({ type: 'ack', roomId, seq: m.seq });
    }
  };
  client.ws.on('message', handler);
  return () => client.ws.off('message', handler);
}

const range1 = (n) => Array.from({ length: n }, (_, i) => i + 1);

/**
 * 让客户端在收到 hasMore=true 的 sync_done 时自动续拉下一批（真实客户端行为，
 * 见 public/index.html 的 sync_done 处理）。返回停止函数。
 */
function autoSync(client, roomId) {
  const handler = (raw) => {
    const m = JSON.parse(raw.toString());
    if (m.type === 'sync_done' && m.roomId === roomId && m.hasMore) {
      client.send({ type: 'sync', roomId, lastSeq: m.lastSeq });
    }
  };
  client.ws.on('message', handler);
  return () => client.ws.off('message', handler);
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

// ---------------------------------------------------------------- 背压：连接维度

test('背压按连接隔离：同用户慢设备被暂停时，快设备仍实时消费', async () => {
  const { server, port } = await startServer({ maxUnackedPerConn: 5 });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const sender = await Client.connect(port, ub.token); // bob 持续发消息
    const slow = await Client.connect(port, ua.token); // alice 设备 1：从不 ACK
    const fast = await Client.connect(port, ua.token); // alice 设备 2：每条都 ACK
    const roomId = await createRoom(sender, 'bp');
    await joinRoom(slow, roomId);
    await joinRoom(fast, roomId);
    // 发送者也会收到自己消息的广播回包，真实客户端会累积 ACK；测试中同样自动确认，
    // 否则发送者自己也会触发连接级背压（广播含回包）干扰场景。
    const stopAutoAckSender = autoAck(sender, roomId);
    const stopAutoAck = autoAck(fast, roomId);
    // 映射到服务端连接对象（按建立顺序：慢设备先连）
    const [slowConn, fastConn] = serverConns(server, ua.userId);

    // 产生 15 条消息
    for (let i = 1; i <= 15; i++) {
      sender.send({ type: 'msg', roomId, clientMsgId: `t${i}`, content: `m${i}` });
    }
    await sender.waitFor((m) => m.type === 'ack' && m.clientMsgId === 't15', 5000);

    // 慢设备在第 6 条处进入暂停
    await withTimeout(
      (async () => { while (!slowConn.paused) await sleep(10); })(),
      3000,
      'slow paused'
    );

    // 快设备实时收齐全部 15 条，且从未被暂停
    await fast.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 15, 5000);
    assert.deepEqual(fast.roomSeqs(roomId), range1(15));
    assert.equal(fastConn.paused, false, '快设备不应被慢设备牵连进入背压');

    // 慢设备只收过高水位以内的消息，未 ACK 积压有界，缺口已被标记待补
    assert.equal(slowConn.unackedCount, 5, '暂停后未 ACK 积压不得继续增长');
    assert.ok(slow.roomSeqs(roomId).length <= 6);
    assert.ok(slowConn.blockedRooms.has(roomId));

    stopAutoAck();
    stopAutoAckSender();
    await sender.close();
    await slow.close();
    await fast.close();
  } finally {
    server.stop();
  }
});

test('慢设备 ACK 排水后自动恢复，暂停期间缺口经重放补齐且不重复', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 5,
    backpressureResumeRatio: 0.4, // 低水位 = 2 条
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const sender = await Client.connect(port, ub.token);
    const slow = await Client.connect(port, ua.token);
    const roomId = await createRoom(sender, 'drain');
    await joinRoom(slow, roomId);
    const stopAutoAckSender = autoAck(sender, roomId); // 发送者同样 ACK 自己的广播回包
    const [slowConn] = serverConns(server, ua.userId);

    // 前 6 条：慢设备收 1..5 后在第 6 条处暂停
    for (let i = 1; i <= 6; i++) {
      sender.send({ type: 'msg', roomId, clientMsgId: `d${i}`, content: `m${i}` });
    }
    await sender.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'd6', 5000);
    await withTimeout(
      (async () => { while (!slowConn.paused) await sleep(10); })(),
      3000,
      'pause'
    );
    assert.deepEqual(slow.roomSeqs(roomId), [1, 2, 3, 4, 5]);

    // 暂停期间再产生 7..10，慢设备实时通道收不到
    for (let i = 7; i <= 10; i++) {
      sender.send({ type: 'msg', roomId, clientMsgId: `d${i}`, content: `m${i}` });
    }
    await sender.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'd10', 5000);
    await sleep(100);
    assert.deepEqual(slow.roomSeqs(roomId), [1, 2, 3, 4, 5]);

    // 排水：累积 ACK 到 seq=4（仅剩 seq5 一条 < 低水位 2）→ 恢复并从投递游标重放。
    // 回放容量 = 高水位 5 - 剩余积压 1 = 4 条：本批只能补 6..9，seq10 再次触顶被截断，
    // sync_done(hasMore=true) —— 与真实客户端一致，ACK 本批后续拉下一批。
    slow.send({ type: 'ack', roomId, seq: 4 });
    const batch1 = await slow.waitFor(
      (m) => m.type === 'sync_done' && m.roomId === roomId && m.hasMore === true,
      5000
    );
    assert.equal(batch1.lastSeq, 9);
    assert.deepEqual(slow.roomSeqs(roomId), range1(9));

    // ACK 掉回放批次（含 seq5..9）排水 → 自动重放最后一条 seq10
    slow.send({ type: 'ack', roomId, seq: 9 });
    const batch2 = await slow.waitFor(
      (m) => m.type === 'sync_done' && m.roomId === roomId && m.hasMore === false,
      5000
    );
    assert.equal(batch2.lastSeq, 10);
    assert.deepEqual(slow.roomSeqs(roomId), range1(10), '暂停期间缺口完整补齐、按序无重复');

    // 恢复后新消息恢复实时投递
    sender.send({ type: 'msg', roomId, clientMsgId: 'd11', content: 'after' });
    await slow.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 11, 3000);
    assert.deepEqual(slow.roomSeqs(roomId), range1(11));

    stopAutoAckSender();
    await sender.close();
    await slow.close();
  } finally {
    server.stop();
  }
});

test('暂停超宽限只断开慢连接：同账号其他设备不断、游标不损坏，慢设备重连补发完好', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 3,
    backpressureTimeoutMs: 300,
    backpressureCheckIntervalMs: 50,
    heartbeatTimeoutMs: 60_000,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const sender = await Client.connect(port, ub.token);
    const slow = await Client.connect(port, ua.token);
    const fast = await Client.connect(port, ua.token);
    const roomId = await createRoom(sender, 'kill');
    await joinRoom(slow, roomId);
    await joinRoom(fast, roomId);
    // 发送者也会收到自己消息的广播回包，真实客户端会累积 ACK；测试中同样自动确认，
    // 否则发送者自己也会触发连接级背压（广播含回包）干扰场景。
    const stopAutoAckSender = autoAck(sender, roomId);
    const stopAutoAck = autoAck(fast, roomId);
    const [slowConn, fastConn] = serverConns(server, ua.userId);

    // 慢设备 3 条未 ACK → 第 4 条时暂停；快设备每条都 ACK（推进 per-user 游标）
    for (let i = 1; i <= 4; i++) {
      sender.send({ type: 'msg', roomId, clientMsgId: `k${i}`, content: `m${i}` });
    }
    await sender.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'k4', 5000);
    await withTimeout(
      (async () => { while (!slowConn.paused) await sleep(10); })(),
      3000,
      'slow pause'
    );
    await fast.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 4, 5000);

    // 等慢设备被宽限超时 terminate
    await withTimeout(slow.closed, 3000, 'slow closed');
    assert.equal(fast.ws.readyState, 1, '同账号快设备连接必须保持 OPEN');
    assert.equal(fastConn.paused, false);

    // 断开只摘慢设备：close 事件清理后，Hub 中同账号仍恰好剩快设备一条连接
    await withTimeout(
      (async () => {
        while (serverConns(server, ua.userId).some((c) => c.id === slowConn.id)) await sleep(10);
      })(),
      2000,
      'slow removed from hub'
    );
    assert.deepEqual(serverConns(server, ua.userId).map((c) => c.id), [fastConn.id]);

    // 慢设备被断开期间继续产生消息，快设备照常实时消费
    sender.send({ type: 'msg', roomId, clientMsgId: 'k5', content: 'm5' });
    await fast.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 5, 5000);

    // 慢设备重连：它此前从未 ACK 过，携带本地进度 lastSeq=0，必须完整收到 1..5。
    // 即使 per-user 游标已被快设备推进到 4/5，也以它自己上报的进度为准，不丢消息。
    // 重连后按真实客户端行为：对收到的 msg 累积 ACK、hasMore 时自动续拉。
    const reborn = await Client.connect(port, ua.token);
    const stopRebornAck = autoAck(reborn, roomId);
    const stopRebornSync = autoSync(reborn, roomId);
    await joinRoom(reborn, roomId, 0);
    await reborn.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId && m.hasMore === false, 5000);
    assert.deepEqual(reborn.roomSeqs(roomId), range1(5));

    stopAutoAck();
    stopAutoAckSender();
    stopRebornAck();
    stopRebornSync();
    await sender.close();
    await fast.close();
    await reborn.close();
  } finally {
    server.stop();
  }
});
