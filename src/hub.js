'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 * unacked: Map<roomId, Map<seq, {frame, lastSent, tries}>> —— 已推送但未被客户端
 * 累积 ACK 确认的消息，超时重发；这是「至少一次投递」的服务端正，配合客户端
 * 按 seq 去重（幂等消费）达到效果上的恰好一次。
 *
 * 背压状态机（所有统计均以「连接」为粒度，同一用户的多台设备互不影响）：
 *   正常 --unackedCount>=maxUnackedPerConn--> 已暂停（丢实时推送、发 backpressure:paused）
 *   已暂停 --ack 排空到 resume 水位--> 正常（发 backpressure:resume，客户端按本地进度 sync 补洞）
 *   已暂停 --持续超过 backpressureDisconnectMs--> 仅断开本连接（1013），重连走 sync
 */
class Connection {
  constructor(ws, user) {
    this.id = nextConnId++;
    this.ws = ws;
    this.userId = user.id;
    this.name = user.name;
    this.connectedAt = now();
    this.lastPong = now(); // 最近一次收到 pong 的时间，心跳判活依据
    this.rooms = new Set(); // 本连接已加入的房间
    this.unacked = new Map();
    this.unackedCount = 0;
    this.paused = false; // 背压暂停中：实时推送对本连接丢弃
    this.pausedSince = 0; // 进入暂停的时间戳（断开宽限期起算点）
    // 暂停期间错过实时推送、需要在 resume 时重同步的房间
    this.pausedRooms = new Set();
  }

  trackUnacked(roomId, seq, frame) {
    let room = this.unacked.get(roomId);
    if (!room) {
      room = new Map();
      this.unacked.set(roomId, room);
    }
    // 同一 seq 重复登记（resume sync 与实时推送交叠、补发重发）只刷新帧内容，
    // 不重复计数 —— 背压判断依赖 unackedCount 的准确
    const already = room.has(seq);
    room.set(seq, { frame, lastSent: now(), tries: 0 });
    if (!already) this.unackedCount++;
  }

  /** 累积 ACK：清除 roomId 下所有 seq <= ackSeq 的未确认项，返回新确认的数量 */
  ack(roomId, ackSeq) {
    const room = this.unacked.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    for (const seq of room.keys()) {
      if (seq <= ackSeq) {
        room.delete(seq);
        cleared++;
      }
    }
    if (room.size === 0) this.unacked.delete(roomId);
    this.unackedCount -= cleared;
    return cleared;
  }

  /** 摘出所有超时未确认、需要重发的条目（暂停期间不重发，避免慢连接被重发流量进一步打爆） */
  *pendingResends(staleMs) {
    const t = now();
    for (const room of this.unacked.values()) {
      for (const entry of room.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
    }
  }
}

/**
 * 连接注册中心：全局/按用户/按房间的连接索引，广播，心跳、背压与重发扫描。
 */
class Hub {
  constructor(config) {
    this.config = config;
    this.all = new Set(); // 全部连接
    this.byUser = new Map(); // userId -> Set<Connection>
    this.byRoom = new Map(); // roomId -> Set<Connection>
  }

  /** 准入控制：全局上限 + 单用户上限。返回 null 表示可接入，否则返回拒绝原因码。 */
  checkAdmission(userId) {
    if (this.all.size >= this.config.maxConnections) return 'SERVER_FULL';
    const mine = this.byUser.get(userId);
    if (mine && mine.size >= this.config.maxConnectionsPerUser) return 'TOO_MANY_DEVICES';
    return null;
  }

  add(conn) {
    this.all.add(conn);
    let set = this.byUser.get(conn.userId);
    if (!set) {
      set = new Set();
      this.byUser.set(conn.userId, set);
    }
    set.add(conn);
  }

  remove(conn) {
    this.all.delete(conn);
    const mine = this.byUser.get(conn.userId);
    if (mine) {
      mine.delete(conn);
      if (mine.size === 0) this.byUser.delete(conn.userId);
    }
    for (const roomId of conn.rooms) this._leaveRoomSet(roomId, conn);
    conn.rooms.clear();
    conn.unacked.clear();
    conn.unackedCount = 0;
    conn.paused = false;
    conn.pausedRooms.clear();
  }

  joinRoom(conn, roomId) {
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
    conn.rooms.add(roomId);
    // 暂停状态下入的房也会持续错过实时推送，统一登记到 resume 重同步集合
    if (conn.paused) conn.pausedRooms.add(roomId);
  }

  leaveRoom(conn, roomId) {
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    conn.pausedRooms.delete(roomId);
    const room = conn.unacked.get(roomId);
    if (room) {
      conn.unackedCount -= room.size;
      conn.unacked.delete(roomId);
    }
    // 离开房间同样可能排空积压：达标即恢复，避免无消息可 ACK 时一直挂起到宽限期断开
    if (conn.paused && conn.unackedCount <= this.resumeThreshold) this._resumeConn(conn);
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  /** 房间内在线用户 ID 列表（去重） */
  onlineUserIds(roomId) {
    const set = this.byRoom.get(roomId);
    if (!set) return [];
    return [...new Set([...set].map((c) => c.userId))];
  }

  // ---------------------------------------------------------------- 背压状态机

  get resumeThreshold() {
    return Math.max(0, Math.floor(this.config.maxUnackedPerConn * this.config.backpressureResumeRatio));
  }

  /**
   * 进入暂停态：只翻转本连接自己的状态；逐房间记录缺口，供 resume 时重同步。
   * 阈值判断放在调用方（发送路径），确保只在积压真正增长时触发一次。
   */
  _pauseConn(conn) {
    conn.paused = true;
    conn.pausedSince = now();
    conn.pausedRooms = new Set(conn.rooms);
    this._sendRaw(conn, JSON.stringify({
      type: 'backpressure',
      state: 'paused',
      limit: this.config.maxUnackedPerConn,
      unacked: conn.unackedCount,
      hint: 'live push paused; keep ACKing, missing messages will be re-synced on resume',
    }));
  }

  /**
   * 排空到恢复水位后恢复实时推送，并通知客户端按其本地进度逐房间重同步缺口。
   * 游标以客户端自报的 lastSeenSeq 为准（sync 处理器既有逻辑），不使用可能被同账号
   * 其他设备推进过的服务端用户游标 —— 各设备独立补齐自己错过的消息。
   */
  _resumeConn(conn) {
    const rooms = [...conn.pausedRooms].filter((r) => conn.rooms.has(r));
    conn.paused = false;
    conn.pausedSince = 0;
    conn.pausedRooms.clear();
    this._sendRaw(conn, JSON.stringify({ type: 'backpressure', state: 'resume', rooms }));
  }

  /**
   * 处理一条累积 ACK 对背压状态的影响：先清本连接的未确认队列，若因此排空到
   * 恢复水位则恢复推送。游标持久化由调用方（server）负责，与本机制解耦。
   * 返回清除的未确认条数。
   */
  applyAck(conn, roomId, ackSeq) {
    const cleared = conn.ack(roomId, ackSeq);
    if (conn.paused && conn.unackedCount <= this.resumeThreshold) this._resumeConn(conn);
    return cleared;
  }

  // ---------------------------------------------------------------- 发送

  /** 不带任何追踪/背压语义的裸发（控制帧：welcome/joined/ack/error/backpressure…） */
  _sendRaw(conn, str) {
    if (conn.ws.readyState !== 1 /* OPEN */) return false;
    try {
      conn.ws.send(str);
    } catch {
      return false;
    }
    return true;
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪（用于 msg 类帧）。
   *
   * 背压策略（按连接维度）：
   * 1. 暂停态：track 帧一律不入队、不发送（实时推送对该连接丢弃，消息已落库，
   *    resume 后由 sync 补齐），非 track 控制帧照常发送；
   * 2. 正常态下未确认积压达到软上限：只对本连接进入暂停（不影响同账号其他设备）；
   * 3. 持续暂停超过宽限期仍未排空：由 backpressureSweep 断开本连接（1013），
   *    客户端重连后走 sync 补发。断开只作用于该 ws，hub 按连接清理，不碰房间、
   *    用户游标与其他连接。
   * 返回 false 表示本帧未投递（暂停/背压/连接不可写）。
   */
  send(conn, frame, { track = false, roomId = null, seq = null } = {}) {
    if (conn.ws.readyState !== 1 /* OPEN */) return false;

    if (track) {
      if (conn.paused) {
        // 实时推送暂停：不缓冲、不追踪，积压因此被钉死在上限附近，内存不会继续增长。
        return false;
      }
      if (conn.unackedCount >= this.config.maxUnackedPerConn) {
        this._pauseConn(conn);
        return false; // 触发暂停的这一帧也丢弃，resume/重连后补
      }
    }

    const str = typeof frame === 'string' ? frame : JSON.stringify(frame);
    if (!this._sendRaw(conn, str)) return false;
    if (track && roomId != null && seq != null) conn.trackUnacked(roomId, seq, str);
    return true;
  }

  /** 广播到房间所有连接（含发送者的其他设备）。frame 只序列化一次。 */
  broadcast(roomId, frame, { track = false, seq = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of set) {
      // 慢连接被暂停时 send 返回 false，仅此连接漏投；同房间/同账号的其他连接正常计数
      if (this.send(conn, str, { track, roomId, seq })) delivered++;
    }
    return delivered;
  }

  /** 心跳扫描：超时未 pong 的连接直接 terminate（触发 close 走正常清理） */
  heartbeatSweep() {
    const t = now();
    for (const conn of this.all) {
      if (t - conn.lastPong > this.config.heartbeatTimeoutMs) {
        conn.ws.terminate();
        continue;
      }
      try {
        conn.ws.ping();
      } catch { /* 连接已损坏，等待 close 事件清理 */ }
    }
  }

  /** 重发扫描：超时未 ACK 的消息重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
  resendSweep() {
    const { ackResendAfterMs, ackMaxResend } = this.config;
    for (const conn of this.all) {
      if (conn.paused) continue; // 暂停期不重发：重发只会给慢连接添堵，缺口交给 resume sync
      for (const entry of conn.pendingResends(ackResendAfterMs)) {
        entry.tries++;
        if (entry.tries > ackMaxResend) {
          conn.ws.close(1011, 'ack timeout');
          break;
        }
        if (conn.ws.readyState === 1) {
          try {
            conn.ws.send(entry.frame);
            entry.lastSent = now();
          } catch { /* 下一轮再处理 */ }
        }
      }
    }
  }

  /**
   * 背压扫描：暂停持续超过宽限期（始终收不到足够 ACK）的连接判定为无以为继，
   * 只关闭这一条连接（1013 Try Again Later）——同账号其他设备、房间成员关系与
   * 服务端游标均不受影响；客户端重连重新 join 后按本地进度 sync 补齐。
   */
  backpressureSweep() {
    const t = now();
    for (const conn of this.all) {
      if (conn.paused && t - conn.pausedSince > this.config.backpressureDisconnectMs) {
        conn.ws.close(1013, 'backpressure: slow consumer, resync after reconnect');
      }
    }
  }

  stats() {
    let paused = 0;
    for (const conn of this.all) if (conn.paused) paused++;
    return {
      connections: this.all.size,
      paused,
      users: this.byUser.size,
      rooms: this.byRoom.size,
    };
  }
}

module.exports = { Hub, Connection };
