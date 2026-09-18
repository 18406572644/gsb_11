'use strict';

/**
 * 全局配置。全部支持环境变量覆盖，便于测试与部署。
 */
module.exports = {
  // 服务监听
  port: Number(process.env.PORT || 8080),
  host: process.env.HOST || '0.0.0.0',

  // SQLite 文件路径，':memory:' 仅用于测试
  dbPath: process.env.CHAT_DB_PATH || 'chat.db',

  // 连接管理
  maxConnections: Number(process.env.MAX_CONNECTIONS || 1000), // 全局最大并发连接
  maxConnectionsPerUser: Number(process.env.MAX_CONNECTIONS_PER_USER || 3), // 单用户最大连接（多端）
  heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS || 30_000), // ping 周期
  heartbeatTimeoutMs: Number(process.env.HEARTBEAT_TIMEOUT_MS || 75_000), // 超过该时长无 pong 判定死亡

  // 可靠投递
  ackResendIntervalMs: Number(process.env.ACK_RESEND_INTERVAL_MS || 2_000), // 未 ACK 重发扫描周期
  ackResendAfterMs: Number(process.env.ACK_RESEND_AFTER_MS || 3_000), // 发送后多久未收到 ACK 触发重发
  ackMaxResend: Number(process.env.ACK_MAX_RESEND || 5), // 单条消息最大重发次数，超限断开连接

  // 背压（一律按「连接」维度独立统计，与同一用户的其他设备互不影响）：
  // 1) 未 ACK 消息数达到 maxUnackedPerConn（高水位）→ 暂停该连接的实时推送（不发、不入队）；
  // 2) 发送缓冲区字节数达到 maxSendBufferBytes（高水位）→ 同样暂停（客户端读得慢但 TCP 未断的场景）；
  // 3) ACK 排水 / 缓冲区回落到 高水位 * backpressureResumeRatio（低水位，滞回防抖）→ 恢复推送，
  //    暂停期间错过的消息按现有 sync 机制（replayRoom + sync_done）从该连接的投递游标补齐；
  // 4) 暂停超过 backpressureTimeoutMs 仍未恢复 → 判定客户端无消费能力，仅 terminate 该连接，
  //    客户端重连后走 join/sync 补发；同一用户的其他设备连接与游标均不受影响。
  maxUnackedPerConn: Number(process.env.MAX_UNACKED_PER_CONN || 1_000), // 单连接未 ACK 积压高水位（暂停阈值）
  maxSendBufferBytes: Number(process.env.MAX_SEND_BUFFER_BYTES || 1_048_576), // 单连接发送缓冲区高水位（默认 1 MiB）
  backpressureResumeRatio: Number(process.env.BACKPRESSURE_RESUME_RATIO || 0.5), // 低水位 = 高水位 × 该比例（滞回）
  backpressureTimeoutMs: Number(process.env.BACKPRESSURE_TIMEOUT_MS || 30_000), // 暂停宽限：超时仍未排水则断开该连接
  backpressureCheckIntervalMs: Number(process.env.BACKPRESSURE_CHECK_INTERVAL_MS || 1_000), // 背压扫描周期（查缓冲区/超时）

  // 消息
  maxContentLength: Number(process.env.MAX_CONTENT_LENGTH || 4_000), // 单条消息最大字符数
  syncBatchSize: Number(process.env.SYNC_BATCH_SIZE || 500), // 断线补发单批最大条数
  historyMaxLimit: Number(process.env.HISTORY_MAX_LIMIT || 100), // 历史消息单次拉取上限

  // 发送限流（令牌桶，按用户）
  rateLimitPerSec: Number(process.env.RATE_LIMIT_PER_SEC || 10),
  rateLimitBurst: Number(process.env.RATE_LIMIT_BURST || 20),

  // 演示用鉴权：token 签名密钥（生产环境务必替换）
  authSecret: process.env.AUTH_SECRET || 'dev-secret-change-me',
};
