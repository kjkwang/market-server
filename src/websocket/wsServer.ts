import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import { CONFIG } from '../config';
import { SessionManager } from './sessionManager';
import { ClientMessage, ServerMessage, MarketData } from '../types';
import { localEventBus } from '../utils/localEventBus';
import { IncomingMessage } from 'http';

export class WsServer {
  private wss: WebSocketServer | null = null;
  private sessionManager: SessionManager;
  private redisSub: Redis | null = null;
  private redisCache: Redis | null = null;
  private useFallback: boolean = false;
  private localListenerRef: ((channel: string, msg: string) => void) | null = null;

  constructor() {
    this.sessionManager = new SessionManager();
    this.initializeRedis();
  }

  private initializeRedis() {
    try {
      const redisOptions = {
        maxRetriesPerRequest: 1,  // 연결 실패 시 여러 번 대기하지 않고 즉시 에러 발생
        retryStrategy: () => null // 재연결을 위해 계속 대기하지 않음 (빠른 Fallback 전환)
      };

      this.redisSub = new Redis(CONFIG.REDIS_URL, redisOptions);
      this.redisCache = new Redis(CONFIG.REDIS_URL, redisOptions);

      const handleRedisError = () => {
        if (!this.useFallback) {
          console.warn('⚠️ WS Server Redis Client Error. Switching to In-Memory Local Pub/Sub Fallback...');
          this.useFallback = true;
          this.setupLocalFallbackListeners();
        }
      };

      this.redisSub.on('error', handleRedisError);
      this.redisCache.on('error', handleRedisError);

      this.redisSub.on('connect', () => {
        console.log('✅ WS Server Redis Sub Client connected.');
        this.useFallback = false;
      });
      this.redisCache.on('connect', () => {
        console.log('✅ WS Server Redis Cache Client connected.');
      });
    } catch (e) {
      console.warn('⚠️ Cannot initialize Redis. Using In-Memory Local Pub/Sub Fallback.');
      this.useFallback = true;
      this.setupLocalFallbackListeners();
    }
  }

  private setupLocalFallbackListeners() {
    if (this.localListenerRef) return;

    CONFIG.MOCK_SYMBOLS.forEach(symbol => {
      const channel = `market:data:${symbol}`;
      localEventBus.on(channel, (message: string) => {
        if (this.useFallback) {
          this.handleRedisMessage(channel, message);
        }
      });
    });

    console.log('Local In-Memory Pub/Sub fallback listeners established.');
  }

  /**
   * approval_key 검증 메소드
   * 값이 'test' 인 경우 성공
   */
  public validateApprovalKey(approvalKey: string): boolean {
    return approvalKey === 'test';
  }

  /**
   * WebSocket 서버 시작
   */
  public async start() {
    this.wss = new WebSocketServer({ 
      port: CONFIG.PORT,
      // Handshake 단계에서 오직 URI 경로(/authorize)만 검증
      verifyClient: (info, callback) => {
        const reqUrl = info.req.url || '';
        const url = new URL(reqUrl, `http://${info.req.headers.host || 'localhost'}`);
        
        if (url.pathname !== CONFIG.WS_PATH) {
          console.warn(`[Handshake Rejected] Invalid path: ${url.pathname}`);
          callback(false, 400, 'Bad Request: Invalid Path');
          return;
        }

        // client_id 쿼리 파라미터 체크 로직 제거됨 (어떤 쿼리 스트링도 요구하지 않음)
        callback(true);
      }
    });

    console.log(`WebSocket Server started on ws://localhost:${CONFIG.PORT}${CONFIG.WS_PATH}`);

    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.handleConnection(ws, req);
    });

    // Redis Pub/Sub 리스너 연결
    if (this.redisSub && !this.useFallback) {
      try {
        await this.redisSub.psubscribe('market:data:*');
        console.log('Redis Subscribed to market:data:* pattern.');
        
        this.redisSub.on('pmessage', (pattern, channel, message) => {
          if (!this.useFallback) {
            this.handleRedisMessage(channel, message);
          }
        });
      } catch (err) {
        console.warn('Failed to subscribe to Redis. Fallback is active.');
      }
    }
  }

  /**
   * 클라이언트 커넥션 핸들러 (연결 시점에는 미인증 상태)
   */
  private handleConnection(ws: WebSocket, req: IncomingMessage) {
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

  /**
   * 한투 스타일의 Custom JSON 메시지 핸들러
   */
  private async handleClientMessage(ws: WebSocket, message: ClientMessage) {
    if (!message.header || !message.body || !message.body.input) {
      this.sendError(ws, 'Missing header or body inside message', 'MISSING_STRUCTURE');
      return;
    }

    const { approval_key, tr_type } = message.header;
    const { tr_key } = message.body.input;

    // 1. approval_key 검증 메소드 수행
    if (!this.validateApprovalKey(approval_key)) {
      console.warn(`[Auth Failed] Invalid approval_key: ${approval_key}`);
      this.sendError(ws, 'Invalid approval_key. Access denied.', 'AUTH_FAILED');
      return;
    }

    // 2. [인증 성공] 현재 세션의 식별자 바인딩 및 중복 세션 단절 체크
    const session = this.sessionManager.getSession(ws);
    if (session && session.clientId !== approval_key) {
      // 이 approval_key로 이미 활동 중인 기존 세션 탐색
      const existingSession = this.sessionManager.getSessionByClientId(approval_key);
      
      if (existingSession && existingSession.ws !== ws) {
        console.warn(`[Duplicate Login] approval_key "${approval_key}" is already registered. Kicking out old connection.`);
        
        // 기존 세션에 경고 통보 후 소켓 종료
        this.sendError(existingSession.ws, 'Duplicate login detected with this approval_key. Disconnected.', 'DUPLICATE_LOGIN');
        existingSession.ws.terminate();
        this.sessionManager.removeSession(existingSession.ws);
      }

      // 새 세션에 Client ID 바인딩
      this.sessionManager.bindClientId(ws, approval_key);
      console.log(`Session bound to clientId (approval_key): [${approval_key}]`);
    }

    if (!tr_key || typeof tr_key !== 'string') {
      this.sendError(ws, 'tr_key (Symbol) is required in body.input', 'SYMBOL_REQUIRED');
      return;
    }

    // 3. tr_type 분기 처리
    if (tr_type === '1') {
      // 실시간 시세 등록/구독
      const result = this.sessionManager.subscribe(ws, tr_key);

      if (!result.success) {
        this.sendError(ws, result.error || 'Subscription failed', 'LIMIT_EXCEEDED');
        return;
      }

      this.sendMessage(ws, {
        type: 'SUBSCRIBED',
        symbol: tr_key,
        currentSubscriptions: result.currentSubscriptions
      });

      console.log(`Client [${approval_key}] subscribed to [${tr_key}]. Total: ${result.currentSubscriptions.length}`);

      // 최초 구독 즉시 최신 시세 갱신용 캐시 탐색
      const cacheKey = `market:cache:${tr_key}`;
      let cachedData: string | null = null;

      if (this.redisCache && !this.useFallback) {
        try {
          cachedData = await this.redisCache.get(cacheKey);
        } catch (e) {
          cachedData = localEventBus.getCache(cacheKey);
        }
      } else {
        cachedData = localEventBus.getCache(cacheKey);
      }

      if (cachedData) {
        try {
          const marketData: MarketData = JSON.parse(cachedData);
          this.sendMessage(ws, {
            type: 'MARKET_DATA',
            symbol: tr_key,
            data: marketData
          });
        } catch (e) {}
      }

    } else if (tr_type === '2') {
      // 실시간 시세 해제
      const result = this.sessionManager.unsubscribe(ws, tr_key);
      
      this.sendMessage(ws, {
        type: 'UNSUBSCRIBED',
        symbol: tr_key,
        currentSubscriptions: result.currentSubscriptions
      });

      console.log(`Client [${approval_key}] unsubscribed from [${tr_key}]. Total: ${result.currentSubscriptions.length}`);
    } else {
      this.sendError(ws, `Unsupported tr_type: ${tr_type}`, 'UNSUPPORTED_TR_TYPE');
    }
  }

  /**
   * Redis/Local Event Bus 시세 수신 시 해당 종목을 구독 중인 연결들에 브로드캐스트
   */
  private handleRedisMessage(channel: string, message: string) {
    const parts = channel.split(':');
    const symbol = parts[parts.length - 1];

    if (!symbol) return;

    const targetSessions = this.sessionManager.getSubscribedSessions(symbol);
    if (targetSessions.length === 0) return;

    try {
      const marketData: MarketData = JSON.parse(message);
      const wsMessage: ServerMessage = {
        type: 'MARKET_DATA',
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

  private sendMessage(ws: WebSocket, message: ServerMessage) {
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
    if (this.wss) {
      this.wss.close();
    }
    
    CONFIG.MOCK_SYMBOLS.forEach(symbol => {
      const channel = `market:data:${symbol}`;
      localEventBus.removeAllListeners(channel);
    });

    if (this.redisSub) {
      try {
        await this.redisSub.quit();
      } catch (e) {}
    }
    if (this.redisCache) {
      try {
        await this.redisCache.quit();
      } catch (e) {}
    }
    console.log('WebSocket Server and Redis connections closed.');
  }
}
