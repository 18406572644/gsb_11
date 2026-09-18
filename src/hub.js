'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 * unacked: Map<roomId, Map<seq, {frame, lastSent, tries}>> —— 已推送但未被客户端
 * 累积 ACK 确认的消息，超时重发；这是「至少一次投递」的服务端正，配合客户端
 * 按 seq 去重（幂等消费）达到效果上的恰好一次。
 *
 * 背压统计严格按「连接」维度：unacked / 发送缓冲区 / 暂停状态全部是连接私有，
 * 同一用户多台设备各自独立计量，一台慢设备被暂停或断开绝不影响其他设备。
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

    // —— 连接级背压状态 ——
    // 暂停期间实时 msg 既不发送也不入 unacked（内存有界），只在 blockedRooms 记房间；
    // roomProgress 记录该连接在每个房间「已发送或已覆盖到」的最大 seq（投递游标），
    // 恢复时由上层据此游标回放缺口，恢复后绝不重发已投递过的消息。
    this.paused = false;
    this.pausedSince = 0;
    this.blockedRooms = new Set();
    this.roomProgress = new Map(); // roomId -> 连续投递水位（<= 该 seq 的消息本连接都已发过）
    this.pauseReason = null;
  }

  trackUnacked(roomId, seq, frame) {
    let room = this.unacked.get(roomId);
    if (!room) {
      room = new Map();
      this.unacked.set(roomId, room);
    }
    // 同 seq 已在追踪（重放与实时推送边界重叠）不重复计数
    if (!room.has(seq)) this.unackedCount++;
    room.set(seq, { frame, lastSent: now(), tries: 0 });
    this.advanceProgress(roomId, seq);
  }

  /** 推进连接在房间内的连续投递水位（仅单调递增） */
  advanceProgress(roomId, seq) {
    const cur = this.roomProgress.get(roomId);
    if (cur === undefined || seq > cur) this.roomProgress.set(roomId, seq);
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

  /** 摘出所有超时未确认、需要重发的条目 */
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
    /**
     * 暂停恢复回调：由 server 注入，(conn) => void。连接从暂停恢复时被调用，
     * 负责按 conn.blockedRooms + conn.roomProgress 回放暂停期间错过的消息。
     */
    this.onDrain = null;
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
    conn.blockedRooms.clear();
    conn.roomProgress.clear();
  }

  joinRoom(conn, roomId) {
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
    conn.rooms.add(roomId);
  }

  leaveRoom(conn, roomId) {
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    const room = conn.unacked.get(roomId);
    if (room) {
      conn.unackedCount -= room.size;
      conn.unacked.delete(roomId);
    }
    conn.blockedRooms.delete(roomId);
    conn.roomProgress.delete(roomId);
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

  /** 发送缓冲区待写字节数（ws 库标准属性；测试替身未实现时按 0 处理） */
  _buffered(conn) {
    return Number(conn.ws.bufferedAmount) || 0;
  }

  /**
   * 背压判定（连接维度）。满足任一高水位即应暂停：
   *  - 未 ACK 消息数 >= maxUnackedPerConn（客户端不确认）
   *  - 发送缓冲区 >= maxSendBufferBytes（客户端读得慢，TCP 堆积）
   * 返回 null 表示未触发，否则返回暂停原因（'unacked' / 'buffer'）。
   */
  _backpressureReason(conn) {
    if (conn.unackedCount >= this.config.maxUnackedPerConn) return 'unacked';
    if (this._buffered(conn) >= this.config.maxSendBufferBytes) return 'buffer';
    return null;
  }

  /** 低水位判定（滞回，避免在阈值附近反复抖动） */
  _underResumeLevel(conn) {
    const { maxUnackedPerConn, maxSendBufferBytes, backpressureResumeRatio } = this.config;
    return (
      conn.unackedCount < maxUnackedPerConn * backpressureResumeRatio &&
      this._buffered(conn) < maxSendBufferBytes * backpressureResumeRatio
    );
  }

  /** 将连接切入暂停态：停止实时推送，等待 ACK/缓冲排水。幂等。 */
  _pause(conn, reason) {
    if (!conn.paused) {
      conn.paused = true;
      conn.pausedSince = now();
      conn.pauseReason = reason;
    }
  }

  /**
   * 尝试恢复推送：处于暂停且已回落到低水位时解除暂停，触发缺口回放。
   * 在收到 ACK、背压扫描排水达标时调用。
   */
  tryResume(conn) {
    if (!conn.paused || !this._underResumeLevel(conn)) return;
    conn.paused = false;
    conn.pausedSince = 0;
    conn.pauseReason = null;
    const blocked = [...conn.blockedRooms];
    conn.blockedRooms.clear();
    if (this.onDrain && blocked.length) {
      try {
        this.onDrain(conn, blocked);
      } catch (err) {
        console.error('[backpressure drain error]', err);
      }
    }
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪（用于 msg 类帧）。
   *
   * 背压处置（仅作用于本连接）：
   *  - 已暂停：track 帧（实时消息）直接跳过——不发送、不入队，只在 blockedRooms 标记缺口房间，
   *    控制帧（track=false）照常发送；
   *  - 未暂停但达到任一高水位：先切入暂停，本帧同样跳过（改由恢复时回放，保证不丢）；
   *  - 其余：正常发送。
   * 返回 true 表示帧已写入连接，false 表示未发送（连接未开 / 触发背压跳过 / 写失败）。
   */
  send(conn, frame, { track = false, roomId = null, seq = null } = {}) {
    if (conn.ws.readyState !== 1 /* OPEN */) return false;

    if (track) {
      if (conn.paused) {
        if (roomId != null) conn.blockedRooms.add(roomId);
        return false;
      }
      const reason = this._backpressureReason(conn);
      if (reason) {
        this._pause(conn, reason);
        if (roomId != null) conn.blockedRooms.add(roomId);
        return false;
      }
    }

    const str = typeof frame === 'string' ? frame : JSON.stringify(frame);
    try {
      conn.ws.send(str);
    } catch {
      return false;
    }
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
      // send 只可能暂停本连接，不会从 set 摘除（terminate 的 close 清理是异步事件），
      // 因此遍历期间集合不变；慢连接跳过后其他连接继续独立投递。
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

  /**
   * 背压扫描（连接维度，逐条独立处置）：
   *  - 暂停中的连接若缓冲区/未 ACK 已回落到低水位 → 恢复并回放缺口；
   *  - 暂停超过宽限期仍无消费能力 → terminate 仅此一条连接（close 事件走 remove 清理），
   *    同一用户的其他设备连接与 byUser 索引、游标均不受影响；
   *  - 未暂停但因缓冲区堆积新触顶的连接 → 立即切入暂停（下一广播周期开始跳过）。
   */
  backpressureSweep() {
    const t = now();
    for (const conn of [...this.all]) {
      if (conn.ws.readyState !== 1) continue;
      if (conn.paused) {
        if (this._underResumeLevel(conn)) {
          this.tryResume(conn);
        } else if (t - conn.pausedSince > this.config.backpressureTimeoutMs) {
          // 慢读者往往连 close 帧都读不走，terminate 立即释放 socket 与本连接全部积压内存；
          // 不触碰 byUser 中的其他连接，客户端重连后凭游标/sync 补发，不丢消息。
          try { conn.ws.terminate(); } catch { /* 已关闭，等 close 清理 */ }
        }
        continue;
      }
      const reason = this._backpressureReason(conn);
      if (reason) this._pause(conn, reason);
    }
  }

  /** 重发扫描：超时未 ACK 的消息重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
  resendSweep() {
    const { ackResendAfterMs, ackMaxResend, maxSendBufferBytes } = this.config;
    for (const conn of this.all) {
      // 连接已被背压暂停（客户端读不动）时不再叠加重发，否则只会把缓冲区堆得更高；
      // 暂停超时由 backpressureSweep 负责断开。
      if (conn.paused) continue;
      for (const entry of conn.pendingResends(ackResendAfterMs)) {
        // 发送缓冲区已经堆满时本轮先不重发也不累计次数，等排水后下一轮再试，
        // 避免向读不动的连接叠加数据、白白耗尽 ackMaxResend。
        if (conn.ws.readyState !== 1 || this._buffered(conn) >= maxSendBufferBytes) continue;
        entry.tries++;
        if (entry.tries > ackMaxResend) {
          conn.ws.close(1011, 'ack timeout');
          break;
        }
        try {
          conn.ws.send(entry.frame);
          entry.lastSent = now();
        } catch { /* 下一轮再处理 */ }
      }
    }
  }

  /** 当前处于背压暂停态的连接数（观测用） */
  pausedCount() {
    let n = 0;
    for (const conn of this.all) if (conn.paused) n++;
    return n;
  }

  stats() {
    return {
      connections: this.all.size,
      users: this.byUser.size,
      rooms: this.byRoom.size,
      paused: this.pausedCount(),
    };
  }
}

module.exports = { Hub, Connection };
