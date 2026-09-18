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

  // 背压（严格按「连接」维度统计与处置，同一用户的多台设备互不影响）：
  // 积压达到软上限后暂停向该连接实时推送（不缓冲，内存不再增长）；
  // 收到 ACK 排空到 maxUnackedPerConn * backpressureResumeRatio 以下后恢复推送，
  // 并通知客户端按本地进度 sync 补齐暂停期间错过的消息；
  // 暂停持续超过宽限期仍未排空，则只断开这条慢连接（1013），重连后走 sync。
  maxUnackedPerConn: Number(process.env.MAX_UNACKED_PER_CONN || 1_000), // 单连接未 ACK 积压软上限
  backpressureResumeRatio: Number(process.env.BACKPRESSURE_RESUME_RATIO || 0.5), // 恢复水位占软上限比例
  backpressureDisconnectMs: Number(process.env.BACKPRESSURE_DISCONNECT_MS || 30_000), // 暂停宽限期，超限断开慢连接
  backpressureSweepMs: Number(process.env.BACKPRESSURE_SWEEP_MS || 2_000), // 背压扫描周期

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
