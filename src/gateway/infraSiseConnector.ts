import Redis from 'ioredis';
import { CONFIG, REDIS_CONFIG } from '../config';
import { TradeData, OrderbookData, OrderbookItem } from '../types';
import { SymbolManager } from '../utils/symbolManager';

export class InfraSiseConnector {
  private redisClient: Redis;
  private intervalId: NodeJS.Timeout | null = null;
  private currentPrices: Map<string, number> = new Map();

  constructor() {
    this.redisClient = new Redis(REDIS_CONFIG);

    console.log('constructor in InfraSiseConnector');

    this.redisClient.on('connect', () => {
      console.log('✅ [InfraSise] Connected to Redis (connect event)');
    });

    this.redisClient.on('ready', () => {
      const addr = (this.redisClient.stream as any)?.remoteAddress || this.redisClient.options.host || 'unknown';
      const port = (this.redisClient.stream as any)?.remotePort || this.redisClient.options.port || 'unknown';
      console.log(`🔗 [InfraSise] Ready - connected to Redis server: ${addr}:${port}`);
    });

    this.redisClient.on('reconnecting', () => {
      console.log('♻️ [InfraSise] Redis client is reconnecting...');
    });

    this.redisClient.on('sentinelError', (err) => {
      console.warn(`⚠️ [InfraSise] Sentinel error: ${err.message}`);
    });

    this.redisClient.on('end', () => {
      console.log('❌ [InfraSise] Connection closed');
    });

    this.redisClient.on('error', (err) => {
      console.warn(`⚠️ [InfraSise] Redis error: ${err.message}`);
    });
  

    this.initializePrices();
  }

  private initializePrices() {
    const basePrices: { [key: string]: number } = {
      '005930': 75000,  // 삼성전자
      '000660': 180000, // SK하이닉스
      '035420': 190000, // NAVER
      '035720': 50000,  // 카카오
      '005380': 250000, // 현대차
    };

    const symbols = SymbolManager.getValidSymbols();
    symbols.forEach(symbol => {
      const startPrice = basePrices[symbol] || Math.floor(Math.random() * 100000) + 10000;
      this.currentPrices.set(symbol, startPrice);
    });
  }

  /**
   * 실시간 체결가(H0STCNT0) 데이터 생성
   */
  private generateTradeData(symbol: string, currentPrice: number): { tradeData: TradeData; newPrice: number } {
    const changePercent = (Math.random() * 4 - 2) / 100; // -2% ~ +2%
    const change = Math.round(currentPrice * changePercent);
    const newPrice = Math.max(100, currentPrice + change);

    const basePrice = 50000;
    const changeFromBase = newPrice - basePrice;
    const changeRate = parseFloat(((changeFromBase / basePrice) * 100).toFixed(2));
    const volume = Math.floor(Math.random() * 1000) + 10;

    const tradeData: TradeData = {
      tr_id: 'H0STCNT0',
      symbol,
      price: newPrice,
      change: changeFromBase,
      changeRate,
      volume,
      timestamp: Date.now(),
    };

    return { tradeData, newPrice };
  }

  /**
   * 실시간 호가(H0STASP0) 데이터 생성 (매도/매수 1~10호가)
   */
  private generateOrderbookData(symbol: string, currentPrice: number): OrderbookData {
    const tick = Math.max(10, Math.floor(currentPrice * 0.001)); // 호가 단위
    const ask: OrderbookItem[] = [];
    const bid: OrderbookItem[] = [];

    let totalAskVol = 0;
    let totalBidVol = 0;

    // 매도호가 (1호가 ~ 10호가)
    for (let i = 1; i <= 10; i++) {
      const price = currentPrice + (tick * i);
      const volume = Math.floor(Math.random() * 5000) + 100;
      ask.push({ price, volume });
      totalAskVol += volume;
    }

    // 매수호가 (1호가 ~ 10호가)
    for (let i = 1; i <= 10; i++) {
      const price = Math.max(100, currentPrice - (tick * i));
      const volume = Math.floor(Math.random() * 5000) + 100;
      bid.push({ price, volume });
      totalBidVol += volume;
    }

    return {
      tr_id: 'H0STASP0',
      symbol,
      ask,
      bid,
      totalAskVolume: totalAskVol,
      totalBidVolume: totalBidVol,
      timestamp: Date.now(),
    };
  }

  /**
   * 가상 시세 피드 발생기 시작
   */
  public startFeed() {
    if (this.intervalId) return;

    console.log('Starting Mock InfraSise Feed Generator (H0STCNT0 & H0STASP0)...');

    this.intervalId = setInterval(async () => {
      const validSymbols = SymbolManager.getValidSymbols();
      if (validSymbols.length === 0) return;

      const count = Math.floor(Math.random() * 4) + 2;

      for (let i = 0; i < count; i++) {
        const randomIndex = Math.floor(Math.random() * validSymbols.length);
        const symbol = validSymbols[randomIndex];
        const currentPrice = this.currentPrices.get(symbol) || 50000;

        // 1. 체결가 피드 (H0STCNT0) 생성
        const { tradeData, newPrice } = this.generateTradeData(symbol, currentPrice);
        this.currentPrices.set(symbol, newPrice);

        const tradeChannel = `market:data:H0STCNT0:${symbol}`;
        const tradeCacheKey = `market:cache:H0STCNT0:${symbol}`;
        const tradePayload = JSON.stringify(tradeData);

        // 2. 호가 피드 (H0STASP0) 생성
        const orderbookData = this.generateOrderbookData(symbol, newPrice);

        const orderbookChannel = `market:data:H0STASP0:${symbol}`;
        const orderbookCacheKey = `market:cache:H0STASP0:${symbol}`;
        const orderbookPayload = JSON.stringify(orderbookData);

        try {
          // Redis Publish & Cache SET (체결가)
          await this.redisClient.publish(tradeChannel, tradePayload);
          await this.redisClient.set(tradeCacheKey, tradePayload);

          // Redis Publish & Cache SET (호가)
          await this.redisClient.publish(orderbookChannel, orderbookPayload);
        } catch (err: any) {
          console.warn(`⚠️ [InfraSise] Publish/Cache error for ${symbol}: ${err?.message || err}`);
        }
      }
    }, 3000);
  }

  /**
   * 가상 피드 발생기 정지
   */
  public stopFeed() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('Mock InfraSise Feed Generator stopped.');
    }
  }

  /**
   * 자원 정리
   */
  public async close() {
    this.stopFeed();
    try {
      await this.redisClient.quit();
    } catch (e) {}
  }
}
