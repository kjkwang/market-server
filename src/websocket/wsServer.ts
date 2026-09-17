import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import { CONFIG, REDIS_CONFIG } from '../config';
import { SessionManager } from './sessionManager';
import { ClientMessage, ServerMessage, MarketData } from '../types';
import { IncomingMessage } from 'http';
import { SymbolManager } from '../utils/symbolManager';

export class WsServer {
  private wss: WebSocketServer | null = null;
  private sessionManager: SessionManager;
  private redisSub: Redis;
  private redisCache: Redis;

  private pingTimerId: NodeJS.Timeout | null = null;

  constructor() {
    this.sessionManager = new SessionManager();

    this.redisSub = new Redis(REDIS_CONFIG);
    this.redisCache = new Redis(REDIS_CONFIG);

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
    this.redisCache.on('error', (err) => console.warn(`⚠️ WS Server Redis Cache error: ${err.message}`));
    this.redisCache.on('sentinelError', (err) => console.warn(`⚠️ WS Server Redis Cache Sentinel error: ${err.message}`));
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
    // Redis Pub/Sub 패턴 구독 (market:data:*)
    await this.redisSub.psubscribe('market:data:*');
    console.log('Redis Subscribed to market:data:* pattern.');

    this.redisSub.on('pmessage', (_pattern, channel, message) => {
      this.handleRedisMessage(channel, message);
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

    console.log(`WebSocket Server started on ws://localhost:${CONFIG.PORT}${CONFIG.WS_PATH}`);

    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.handleConnection(ws, req);
    });

    this.startPingPongLoop();
    console.log(`PingPong loop started (interval: ${CONFIG.PINGPONG_INTERVAL_MS / 1000}s, max miss: ${CONFIG.PINGPONG_MAX_MISS_COUNT}).`);
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

    ws.on('close', () => {
      const removed = this.sessionManager.removeSession(ws);
      if (removed) {
        const idStr = removed.clientId ? `[${removed.clientId}]` : '(Anonymous)';
        console.log(`Client ${idStr} disconnected. Total clients: ${this.sessionManager.getSessionCount()}`);
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
      const existingSession = this.sessionManager.getSessionByClientId(approval_key);

      if (existingSession && existingSession.ws !== ws) {
        console.warn(`[Duplicate Login] approval_key "${approval_key}" is already registered. Kicking out old connection.`);
        this.sendError(existingSession.ws, 'Duplicate login detected with this approval_key. Disconnected.', 'DUPLICATE_LOGIN');
        existingSession.ws.terminate();
        this.sessionManager.removeSession(existingSession.ws);
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
    if (this.pingTimerId) {
      clearInterval(this.pingTimerId);
      this.pingTimerId = null;
    }

    if (this.wss) {
      this.wss.close();
    }

    try {
      await this.redisSub.quit();
    } catch (e) {}

    try {
      await this.redisCache.quit();
    } catch (e) {}

    console.log('WebSocket Server and Redis connections closed.');
  }
}
