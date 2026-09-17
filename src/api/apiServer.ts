import express, { Request, Response } from 'express';
import Redis from 'ioredis';
import { CONFIG, REDIS_CONFIG, REDIS_READ_CONFIG, REDIS_READ_ROLE } from '../config';
import { MarketData } from '../types';
import { SymbolManager } from '../utils/symbolManager';

export class ApiServer {
  private app = express();
  private redisClient: Redis;
  private server: ReturnType<typeof this.app.listen> | null = null;

  constructor() {
    this.redisClient = new Redis(REDIS_READ_CONFIG);

    this.redisClient.on('connect', () => console.log('✅ API Server Redis Client connected.'));
    this.redisClient.on('ready', () => {
      const addr = (this.redisClient.stream as any)?.remoteAddress || 'unknown';
      const port = (this.redisClient.stream as any)?.remotePort || 'unknown';
      console.log(`🔗 API Server Redis Ready - connected to: ${addr}:${port}`);
    });
    this.redisClient.on('error', (err) => {
      if (err.message.includes('invalid reply') && (this.redisClient.options as any).role === 'slave') {
        console.warn('⚠️ [Redis Read Fallback] Sentinel returned no Replica. Switching role to Master for API reads...');
        (this.redisClient.options as any).role = 'master';
      } else {
        console.warn(`⚠️ API Server Redis error: ${err.message}`);
      }
    });

    this.redisClient.on('sentinelError', (err) => {
      if (err.message.includes('invalid reply') && (this.redisClient.options as any).role === 'slave') {
        console.warn('⚠️ [Redis Read Fallback] No active Replica node registered in Sentinel. Automatically falling back to Master for API reads...');
        (this.redisClient.options as any).role = 'master';
      } else {
        console.warn(`⚠️ API Server Sentinel error: ${err.message}`);
      }
    });

    this.setupMiddleware();
    this.setupRoutes();
  }

  private checkTcpPort(host: string, port: number, timeoutMs = 1000): Promise<{ reachable: boolean; latencyMs?: number; error?: string }> {
    return new Promise((resolve) => {
      const net = require('net');
      const start = Date.now();
      const socket = new net.Socket();

      socket.setTimeout(timeoutMs);

      socket.on('connect', () => {
        const latencyMs = Date.now() - start;
        socket.destroy();
        resolve({ reachable: true, latencyMs });
      });

      socket.on('timeout', () => {
        socket.destroy();
        resolve({ reachable: false, error: 'ETIMEDOUT' });
      });

      socket.on('error', (err: any) => {
        socket.destroy();
        resolve({ reachable: false, error: err.message });
      });

      socket.connect(port, host);
    });
  }

  private setupMiddleware() {
    this.app.use(express.json());

    this.app.use((_req, res, next) => {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET');
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      next();
    });
  }

  private setupRoutes() {
    /**
     * GET /api/health
     * Redis Master, Replica, 및 Sentinel 노드들의 포트/핑 모니터링 상태 응답
     */
    this.app.get('/api/health', async (_req: Request, res: Response) => {
      const startTime = Date.now();

      // 1. Redis Client (Master/Read) 핑 검사
      let redisStatus = 'down';
      let redisLatencyMs = -1;
      try {
        const pingStart = Date.now();
        await this.redisClient.ping();
        redisLatencyMs = Date.now() - pingStart;
        redisStatus = 'up';
      } catch (e) {}

      // 2. Sentinel 노드별 TCP 26379 포트 감시
      const sentinelsConfig = REDIS_CONFIG.sentinels || [];
      const sentinelChecks = await Promise.all(
        sentinelsConfig.map(async (sentinel) => {
          const host = sentinel.host || '127.0.0.1';
          const port = sentinel.port || 26379;
          const check = await this.checkTcpPort(host, port, 1000);
          return {
            host,
            port,
            status: check.reachable ? 'up' : 'down',
            ...(check.latencyMs !== undefined ? { latencyMs: check.latencyMs } : {}),
            ...(check.error ? { error: check.error } : {}),
          };
        })
      );

      const isAllHealthy = redisStatus === 'up' && sentinelChecks.every(s => s.status === 'up');
      const isDegraded = redisStatus === 'up' && sentinelChecks.some(s => s.status === 'down');
      const overallStatus = isAllHealthy ? 'ok' : (isDegraded ? 'degraded' : 'down');

      const connectedAddr = (this.redisClient.stream as any)?.remoteAddress || 'unknown';
      const connectedPort = (this.redisClient.stream as any)?.remotePort || 'unknown';
      const activeRole = (this.redisClient.options as any)?.role || 'unknown';

      const responsePayload = {
        success: overallStatus !== 'down',
        status: overallStatus,
        serverId: CONFIG.SERVER_ID,
        uptime: Math.floor(process.uptime()),
        timestamp: new Date().toISOString(),
        durationMs: Date.now() - startTime,
        redis: {
          status: redisStatus,
          configuredRole: REDIS_READ_ROLE,
          activeRole,
          connectedAddress: `${connectedAddr}:${connectedPort}`,
          latencyMs: redisLatencyMs,
        },
        sentinels: sentinelChecks,
      };

      const statusCode = overallStatus === 'down' ? 530 : 200;
      res.status(statusCode).json(responsePayload);
    });

    /**
     * GET /api/market/cache/:tr_id
     * 특정 TR ID(H0STCNT0: 체결가, H0STASP0: 호가)의 전체 종목 최신 시세 목록 조회
     */
    this.app.get('/api/market/cache/:tr_id', async (req: Request, res: Response) => {
      const trId = req.params['tr_id'] as string;
      const validTrIds = ['H0STCNT0', 'H0STASP0'];

      if (!validTrIds.includes(trId)) {
        res.status(400).json({
          success: false,
          message: `Invalid tr_id '${trId}'. Supported: ${validTrIds.join(', ')}`,
        });
        return;
      }

      try {
        const validSymbols = SymbolManager.getValidSymbols();
        const keys = validSymbols.map(symbol => `market:cache:${trId}:${symbol}`);
        const values = await this.redisClient.mget(...keys);

        const data: MarketData[] = [];
        values.forEach((val) => {
          if (val) {
            try {
              data.push(JSON.parse(val) as MarketData);
            } catch {}
          }
        });

        res.json({
          success: true,
          tr_id: trId,
          count: data.length,
          data,
        });
      } catch (err) {
        res.status(503).json({
          success: false,
          message: 'Redis unavailable',
        });
      }
    });

    /**
     * GET /api/market/cache/:tr_id/:symbol
     * 특정 TR ID와 특정 종목코드의 최신 시세 조회
     */
    this.app.get('/api/market/cache/:tr_id/:symbol', async (req: Request, res: Response) => {
      const trId = req.params['tr_id'] as string;
      const symbol = req.params['symbol'] as string;
      const validTrIds = ['H0STCNT0', 'H0STASP0'];

      if (!validTrIds.includes(trId)) {
        res.status(400).json({
          success: false,
          message: `Invalid tr_id '${trId}'. Supported: ${validTrIds.join(', ')}`,
        });
        return;
      }

      if (!SymbolManager.isValidSymbol(symbol)) {
        res.status(404).json({
          success: false,
          message: `Symbol '${symbol}' is invalid.`,
        });
        return;
      }

      try {
        const cacheKey = `market:cache:${trId}:${symbol}`;
        const value = await this.redisClient.get(cacheKey);

        if (!value) {
          res.status(404).json({
            success: false,
            message: `No cache data available for '${trId}:${symbol}' yet.`,
          });
          return;
        }

        res.json({
          success: true,
          tr_id: trId,
          symbol,
          data: JSON.parse(value) as MarketData,
        });
      } catch (err) {
        res.status(503).json({
          success: false,
          message: 'Redis unavailable',
        });
      }
    });

    this.app.use((_req: Request, res: Response) => {
      res.status(404).json({
        success: false,
        message: 'Not Found',
      });
    });
  }

  public start() {
    this.server = this.app.listen(CONFIG.API_PORT, () => {
      console.log(`HTTP API Server started on http://localhost:${CONFIG.API_PORT}`);
      console.log(`  - GET http://localhost:${CONFIG.API_PORT}/api/health`);
      console.log(`  - GET http://localhost:${CONFIG.API_PORT}/api/market/cache/:tr_id`);
      console.log(`  - GET http://localhost:${CONFIG.API_PORT}/api/market/cache/:tr_id/:symbol`);
    });
  }

  public async close() {
    this.server?.close();
    try {
      await this.redisClient.quit();
    } catch {}
    console.log('API Server closed.');
  }
}
