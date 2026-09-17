import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import { CONFIG, REDIS_CONFIG, REDIS_READ_CONFIG, REDIS_READ_ROLE } from '../config';
import { SessionManager } from './sessionManager';
import { ClientMessage, ServerMessage, MarketData } from '../types';
import { IncomingMessage } from 'http';
import { SymbolManager } from '../utils/symbolManager';

export class WsServer {
  private wss: WebSocketServer | null = null;
  private sessionManager: SessionManager;
  private redisSub: Redis;
  private redisCache: Redis;
  private redisWrite: Redis;
  private serverId: string;

  private pingTimerId: NodeJS.Timeout | null = null;

  constructor() {
    this.serverId = CONFIG.SERVER_ID;
    this.sessionManager = new SessionManager();

    // Redis 커넥션 분리:
    // 1. redisWrite: 쓰기 권한을 갖는 Master 노드 전용 (세션 등록, Kick 신호 발행용)
    // 2. redisSub: Pub/Sub 전용 커넥션 (시세 수신 및 Cross-Server Kick 신호 수신용)
    // 3. redisCache: 읽기 전용 작업 (Replica/Slave 설정 준수)
    this.redisWrite = new Redis(REDIS_CONFIG);
    this.redisSub = new Redis(REDIS_CONFIG);
    this.redisCache = new Redis(REDIS_READ_CONFIG);

    this.redisWrite.on('connect', () => console.log(`✅ WS Server Redis Write Client (Master) connected. [Server ID: ${this.serverId}]`));
    this.redisWrite.on('error', (err) => console.warn(`⚠️ WS Server Redis Write error: ${err.message}`));
    this.redisWrite.on('sentinelError', (err) => console.warn(`⚠️ WS Server Redis Write Sentinel error: ${err.message}`));

    this.redisSub.on('connect', () => console.log('✅ WS Server Redis Sub Client connected.'));
    this.redisSub.on('ready', () => {
      const addr = (this.redisSub.stream as any)?.remoteAddress || 'unknown';
      const port = (this.redisSub.stream as any)?.remotePort || 'unknown';
      console.log(`🔗 WS Server Redis Sub Ready - connected to: ${addr}:${port}`);
    });
    this.redisSub.on('error', (err) => console.warn(`⚠️ WS Server Redis Sub error: ${err.message}`));
    this.redisSub.on('sentinelError', (err) => console.warn(`⚠️ WS Server Redis Sub Sentinel error: ${err.message}`));

    this.redisCache.on('connect', () => console.log('✅ WS Server Redis Cache Client connected.'));
    this.redisCache.on('ready', () => {
      const addr = (this.redisCache.stream as any)?.remoteAddress || 'unknown';
      const port = (this.redisCache.stream as any)?.remotePort || 'unknown';
      console.log(`🔗 WS Server Redis Cache Ready - connected to: ${addr}:${port}`);
    });
    this.redisCache.on('error', (err) => {
      if (err.message.includes('invalid reply') && (this.redisCache.options as any).role === 'slave') {
        console.warn('⚠️ [Redis Read Fallback] Sentinel returned no Replica. Switching role to Master...');
        (this.redisCache.options as any).role = 'master';
        this.scheduleReplicaRecoveryCheck();
      } else {
        console.warn(`⚠️ WS Server Redis Cache error: ${err.message}`);
      }
    });

    this.redisCache.on('sentinelError', (err) => {
      if (err.message.includes('invalid reply') && (this.redisCache.options as any).role === 'slave') {
        console.warn('⚠️ [Redis Read Fallback] No active Replica node registered in Sentinel. Automatically falling back to Master for Cache reads...');
        (this.redisCache.options as any).role = 'master';
        this.scheduleReplicaRecoveryCheck();
      } else {
        console.warn(`⚠️ WS Server Redis Cache Sentinel error: ${err.message}`);
      }
    });
  }

  private replicaRecoveryTimer: NodeJS.Timeout | null = null;

  /**
   * Replica 장애 복구 시 WebSocket 클라이언트 단절 없이 백그라운드에서 Replica로 자동 원복(Recovery)
   */
  private scheduleReplicaRecoveryCheck() {
    if (this.replicaRecoveryTimer || REDIS_READ_ROLE !== 'slave') return;

    console.log('🔄 [Replica Recovery Watcher] Started background watcher for Replica recovery (30s interval)...');

    this.replicaRecoveryTimer = setInterval(async () => {
      if ((this.redisCache.options as any).role !== 'master') return;

      try {
        // 복구 여부 확인을 위해 일시적으로 role: slave 테스트
        (this.redisCache.options as any).role = 'slave';
        await this.redisCache.disconnect();
        await this.redisCache.connect();

        console.log('✅ [Redis Read Recovery] Replica node recovered! Seamlessly switched Cache reads back to Replica (WebSocket clients remain 100% connected).');
        if (this.replicaRecoveryTimer) {
          clearInterval(this.replicaRecoveryTimer);
          this.replicaRecoveryTimer = null;
        }
      } catch (e) {
        // 아직 Replica가 복구되지 않은 경우 Master 상태 유지
        (this.redisCache.options as any).role = 'master';
      }
    }, 30000);
  }

  private getFormattedDateTime(): string {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    return `${year}${month}${day}${hours}${minutes}${seconds}`;
  }

  private startPingPongLoop() {
    if (this.pingTimerId) return;

    this.pingTimerId = setInterval(() => {
      const activeSessions = this.sessionManager.getAllSessions();
      const nowStr = this.getFormattedDateTime();

      for (const session of activeSessions) {
        const missedCount = this.sessionManager.incrementMissedPing(session.ws);
        const clientIdStr = session.clientId ? `[${session.clientId}]` : '(Anonymous)';

        if (missedCount >= CONFIG.PINGPONG_MAX_MISS_COUNT) {
          console.warn(`⚠️ [PingPong Timeout] Client ${clientIdStr} failed to respond ${missedCount} times. Terminating connection...`);
          session.ws.terminate();
          this.sessionManager.removeSession(session.ws);
        } else {
          const pingMessage = {
            header: {
              tr_id: 'PINGPONG',
              datetime: nowStr
            }
          };
          this.sendMessage(session.ws, pingMessage);
        }
      }
    }, CONFIG.PINGPONG_INTERVAL_MS);
  }

  public validateApprovalKey(approvalKey: string): boolean {
    return true;
  }

  /**
   * WebSocket 서버 시작
   */
  public async start() {
    // Redis Pub/Sub 패턴 구독 (market:data:*) 및 이중화 서버 간 Kick 신호 채널 (session:kick)
    await this.redisSub.psubscribe('market:data:*');
    await this.redisSub.subscribe('session:kick');
    console.log('Redis Subscribed to market:data:* pattern and session:kick channel.');

    this.redisSub.on('pmessage', (_pattern, channel, message) => {
      this.handleRedisMessage(channel, message);
    });

    this.redisSub.on('message', (channel, message) => {
      if (channel === 'session:kick') {
        this.handleSessionKickMessage(message);
      }
    });

    this.wss = new WebSocketServer({
      port: CONFIG.PORT,
      verifyClient: (info, callback) => {
        const reqUrl = info.req.url || '';
        const url = new URL(reqUrl, `http://${info.req.headers.host || 'localhost'}`);

        if (url.pathname !== CONFIG.WS_PATH) {
          console.warn(`[Handshake Rejected] Invalid path: ${url.pathname}`);
          callback(false, 400, 'Bad Request: Invalid Path');
          return;
        }

        callback(true);
      }
    });

    console.log(`WebSocket Server [${this.serverId}] started on ws://localhost:${CONFIG.PORT}${CONFIG.WS_PATH}`);

    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.handleConnection(ws, req);
    });

    this.startPingPongLoop();
    console.log(`PingPong loop started (interval: ${CONFIG.PINGPONG_INTERVAL_MS / 1000}s, max miss: ${CONFIG.PINGPONG_MAX_MISS_COUNT}).`);
  }

  /**
   * 이중화된 다른 WS 서버로부터 중복 세션 KICK 신호 수신 시 처리
   */
  private handleSessionKickMessage(messageStr: string) {
    try {
      const data = JSON.parse(messageStr);
      const { clientId, senderServerId } = data;

      // 자신이 발행한 Kick 신호인 경우 무시 (자신은 발행 직전 이미 로컬 기존 세션을 정리하고 새 세션을 바인딩했기 때문)
      if (senderServerId === this.serverId) {
        return;
      }

      const existingSession = this.sessionManager.getSessionByClientId(clientId);
      if (existingSession) {
        console.warn(`[Multi-Server Kick] Duplicate login for approval_key "${clientId}" detected from server [${senderServerId}]. Terminating local session.`);
        this.sendError(existingSession.ws, 'Duplicate login detected with this approval_key from another connection/server. Disconnected.', 'DUPLICATE_LOGIN');
        existingSession.ws.terminate();
        this.sessionManager.removeSession(existingSession.ws);
      }
    } catch (err) {
      console.error('Failed to parse session:kick message:', err);
    }
  }

  private handleConnection(ws: WebSocket, _req: IncomingMessage) {
    console.log('New WebSocket client connected (Anonymous).');
    this.sessionManager.addSession(ws);

    ws.on('message', async (data: string) => {
      try {
        const message: ClientMessage = JSON.parse(data);
        await this.handleClientMessage(ws, message);
      } catch (err) {
        console.error('Failed to parse client message:', err);
        this.sendError(ws, 'Invalid JSON format', 'INVALID_JSON');
      }
    });

    ws.on('close', async () => {
      const removed = this.sessionManager.removeSession(ws);
      if (removed) {
        const idStr = removed.clientId ? `[${removed.clientId}]` : '(Anonymous)';
        console.log(`Client ${idStr} disconnected. Total clients: ${this.sessionManager.getSessionCount()}`);

        if (removed.clientId && !CONFIG.ALLOW_MULTIPLE_CONNECTIONS) {
          try {
            await this.redisWrite.del(`session:${removed.clientId}`);
          } catch (e) {}
        }
      }
    });

    ws.on('error', (err) => {
      console.error('WebSocket error on session:', err);
    });
  }

  private async handleClientMessage(ws: WebSocket, message: any) {
    if (message.header?.tr_id === 'PINGPONG') {
      const session = this.sessionManager.getSession(ws);
      const clientIdStr = session?.clientId ? `[${session.clientId}]` : '(Anonymous)';
      this.sessionManager.resetMissedPing(ws);

      // 세션 유지 시간 갱신 (1계정 1연결 모드 시)
      if (session?.clientId && !CONFIG.ALLOW_MULTIPLE_CONNECTIONS) {
        try {
          await this.redisWrite.expire(`session:${session.clientId}`, 60);
        } catch (e) {}
      }

      console.log(`🏓 [PingPong] PONG received from ${clientIdStr}. Miss count reset.`);
      return;
    }

    if (!message.header || !message.body || !message.body.input) {
      this.sendError(ws, 'Missing header or body inside message', 'MISSING_STRUCTURE');
      return;
    }

    const { approval_key, tr_type } = message.header;
    const { tr_id = 'H0STCNT0', tr_key } = message.body.input;

    if (!this.validateApprovalKey(approval_key)) {
      console.warn(`[Auth Failed] Invalid approval_key: ${approval_key}`);
      this.sendError(ws, 'Invalid approval_key. Access denied.', 'AUTH_FAILED');
      return;
    }

    // TR ID 지원 검증 (H0STCNT0: 체결가, H0STASP0: 호가)
    const validTrIds = ['H0STCNT0', 'H0STASP0'];
    if (!validTrIds.includes(tr_id)) {
      this.sendError(ws, `Unsupported tr_id: ${tr_id}. Supported: ${validTrIds.join(', ')}`, 'UNSUPPORTED_TR_ID');
      return;
    }

    const session = this.sessionManager.getSession(ws);
    if (session && session.clientId !== approval_key) {
      // 1계정 1연결 전용 모드 (ALLOW_MULTIPLE_CONNECTIONS 가 false 인 경우)
      if (!CONFIG.ALLOW_MULTIPLE_CONNECTIONS) {
        // A. 로컬 서버 인스턴스 내 기존 동일 세션 강퇴
        const existingSession = this.sessionManager.getSessionByClientId(approval_key);

        if (existingSession && existingSession.ws !== ws) {
          console.warn(`[Duplicate Login] approval_key "${approval_key}" is already registered locally. Kicking out old connection.`);
          this.sendError(existingSession.ws, 'Duplicate login detected with this approval_key. Disconnected.', 'DUPLICATE_LOGIN');
          existingSession.ws.terminate();
          this.sessionManager.removeSession(existingSession.ws);
        }

        // B. 이중화된 타 WS 서버들의 기존 동일 세션 강퇴를 위해 Redis Master로 Kick 신호 발행 + Redis 세션 레지스트리 저장
        try {
          await this.redisWrite.publish('session:kick', JSON.stringify({
            clientId: approval_key,
            senderServerId: this.serverId,
          }));
          await this.redisWrite.set(`session:${approval_key}`, this.serverId, 'EX', 60);
        } catch (err) {
          console.warn(`⚠️ [Redis Master Write] Failed to manage session in Redis Master: ${(err as Error).message}`);
        }
      } else {
        console.log(`[Multi-Connection Allowed] Client approval_key [${approval_key}] connected (Multiple connections mode).`);
      }

      this.sessionManager.bindClientId(ws, approval_key);
      console.log(`Session bound to clientId (approval_key): [${approval_key}]`);
    }

    if (!tr_key || typeof tr_key !== 'string') {
      this.sendError(ws, 'tr_key (Symbol) is required in body.input', 'SYMBOL_REQUIRED');
      return;
    }

    if (!SymbolManager.isValidSymbol(tr_key)) {
      console.warn(`[Invalid Symbol] Requested symbol '${tr_key}' is invalid. Client: [${approval_key}]`);
      this.sendError(ws, `Invalid symbol code: ${tr_key}`, 'INVALID_SYMBOL');
      return;
    }

    if (tr_type === '1') {
      // 실시간 시세 등록/구독 (tr_id + tr_key)
      const result = this.sessionManager.subscribe(ws, tr_id, tr_key);

      if (!result.success) {
        this.sendError(ws, result.error || 'Subscription failed', 'LIMIT_EXCEEDED');
        return;
      }

      const responseHeader = {
        tr_id,
        tr_key,
        encrypt: 'N'
      };

      const responseBody = {
        rt_cd: '0',
        msg_cd: 'OPSP0000',
        msg1: 'SUBSCRIBE SUCCESS',
        output: {
          iv: 'ea654fa68b90da82',
          key: 'wmxignlrmfbasxxjdqqgjcckmvlfdcnc'
        }
      };

      this.sendMessage(ws, {
        header: responseHeader,
        body: responseBody,
        type: 'SUBSCRIBED',
        tr_id,
        symbol: tr_key,
        currentSubscriptions: result.currentSubscriptions
      });

      console.log(`Client [${approval_key}] subscribed to [${tr_id}:${tr_key}]. Total: ${result.currentSubscriptions.length}`);

      // 최초 구독 즉시 Redis 캐시에서 최신 데이터 조회하여 전송
      try {
        const cacheKey = `market:cache:${tr_id}:${tr_key}`;
        const cachedData = await this.redisCache.get(cacheKey);
        if (cachedData) {
          const marketData: MarketData = JSON.parse(cachedData);
          this.sendMessage(ws, {
            type: 'MARKET_DATA',
            tr_id,
            symbol: tr_key,
            data: marketData
          });
        }
      } catch (e) {
        // 캐시 조회 실패 시 무시
      }

    } else if (tr_type === '2') {
      // 실시간 시세 해제
      const result = this.sessionManager.unsubscribe(ws, tr_id, tr_key);

      this.sendMessage(ws, {
        header: {
          tr_id,
          tr_key,
          encrypt: 'N'
        },
        body: {
          rt_cd: '0',
          msg_cd: 'OPSP0000',
          msg1: 'UNSUBSCRIBE SUCCESS',
          output: {
            iv: 'ea654fa68b90da82',
            key: 'wmxignlrmfbasxxjdqqgjcckmvlfdcnc'
          }
        },
        type: 'UNSUBSCRIBED',
        tr_id,
        symbol: tr_key,
        currentSubscriptions: result.currentSubscriptions
      });

      console.log(`Client [${approval_key}] unsubscribed from [${tr_id}:${tr_key}]. Total: ${result.currentSubscriptions.length}`);
    } else {
      this.sendError(ws, `Unsupported tr_type: ${tr_type}`, 'UNSUPPORTED_TR_TYPE');
    }
  }

  /**
   * Redis Pub/Sub 메시지 처리
   * 채널 형태: market:data:{tr_id}:{symbol} (e.g. market:data:H0STCNT0:005930)
   */
  private handleRedisMessage(channel: string, message: string) {
    const parts = channel.split(':');
    if (parts.length < 4) return;

    const trId = parts[2];
    const symbol = parts[3];

    if (!trId || !symbol) return;

    const targetSessions = this.sessionManager.getSubscribedSessions(trId, symbol);
    if (targetSessions.length === 0) return;

    try {
      const marketData: MarketData = JSON.parse(message);
      const wsMessage: ServerMessage = {
        type: 'MARKET_DATA',
        tr_id: trId,
        symbol,
        data: marketData
      };

      const payload = JSON.stringify(wsMessage);

      targetSessions.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(payload);
        }
      });
    } catch (err) {
      console.error(`Failed to parse market data for channel ${channel}:`, err);
    }
  }

  private sendMessage(ws: WebSocket, message: any) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  private sendError(ws: WebSocket, errMsg: string, code: string) {
    this.sendMessage(ws, {
      type: 'ERROR',
      message: errMsg,
      code
    });
  }

  public async close() {
    if (this.replicaRecoveryTimer) {
      clearInterval(this.replicaRecoveryTimer);
      this.replicaRecoveryTimer = null;
    }

    if (this.pingTimerId) {
      clearInterval(this.pingTimerId);
      this.pingTimerId = null;
    }

    if (this.wss) {
      this.wss.close();
    }

    try {
      await this.redisWrite.quit();
    } catch (e) {}

    try {
      await this.redisSub.quit();
    } catch (e) {}

    try {
      await this.redisCache.quit();
    } catch (e) {}

    console.log('WebSocket Server and Redis connections closed.');
  }
}
