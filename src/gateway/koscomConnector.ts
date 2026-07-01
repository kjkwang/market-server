import Redis from 'ioredis';
import { CONFIG } from '../config';
import { MarketData } from '../types';
import { localEventBus } from '../utils/localEventBus';

export class KoscomConnector {
  private redisClient: Redis | null = null;
  private intervalId: NodeJS.Timeout | null = null;
  private currentPrices: Map<string, number> = new Map();
  private useFallback: boolean = false;

  constructor() {
    try {
      this.redisClient = new Redis(CONFIG.REDIS_URL, {
        maxRetriesPerRequest: 1, // 실패 시 빠른 Fallback 전환을 위해 1회 재시도 후 에러 발생
        retryStrategy: () => null // 재시도 안함 (Local fallback 우선)
      });
      
      this.redisClient.on('error', (err) => {
        if (!this.useFallback) {
          console.warn('⚠️ KoscomConnector Redis Client Error. Switching to In-Memory Local Pub/Sub Fallback...');
          this.useFallback = true;
        }
      });

      this.redisClient.on('connect', () => {
        console.log('✅ KoscomConnector connected to Redis.');
        this.useFallback = false;
      });
    } catch (e) {
      console.warn('⚠️ Cannot initialize Redis. Using In-Memory Local Pub/Sub Fallback.');
      this.useFallback = true;
    }

    // 초기 가격 설정
    this.initializePrices();
  }

  private initializePrices() {
    const basePrices: { [key: string]: number } = {
      '005930': 75000, // 삼성전자
      '000660': 180000, // SK하이닉스
      '035420': 190000, // NAVER
      '035720': 50000,  // 카카오
      '005380': 250000, // 현대차
    };

    CONFIG.MOCK_SYMBOLS.forEach(symbol => {
      const startPrice = basePrices[symbol] || Math.floor(Math.random() * 100000) + 10000;
      this.currentPrices.set(symbol, startPrice);
    });
  }

  /**
   * 가상 코스콤 시세 피드 발생기 시작
   */
  public startFeed() {
    if (this.intervalId) return;  //이미실행중이면 중복 방지
    
    console.log('Starting Mock Koscom Feed Generator...');

    this.intervalId = setInterval(async () => {
      const count = Math.floor(Math.random() * 4) + 2;  
      //2부터 5까지의 무작위 정수(2, 3, 4, 5) 중 하나를 무작위로 생성하는 JavaScript 코드입니다.
      
      for (let i = 0; i < count; i++) { 
        //가상 데이터 생성기에서 자주 쓰이는 로직으로, "전체 자산 목록 중 무작위로 하나를 골라 현재 가격을 가져오되, 가격 정보가 없으면 기본값(50,000)을 적용하는 코드"입니다.
        const randomIndex = Math.floor(Math.random() * CONFIG.MOCK_SYMBOLS.length); 
        //추후 실제 주가 종목을 가져오는 놈이 필요하겠네.
        const symbol = CONFIG.MOCK_SYMBOLS[randomIndex];
        const currentPrice = this.currentPrices.get(symbol) || 50000;

        const changePercent = (Math.random() * 4 - 2) / 100; //금융 시뮬레이션이나 주가 차트 생성기에서 -2%에서 +2% 사이의 무작위 가격 변동률(소수점 형태)을 구하는 코드입니다.
        const change = Math.round(currentPrice * changePercent);
        const newPrice = Math.max(100, currentPrice + change);
        this.currentPrices.set(symbol, newPrice);

        const basePrice = 50000;
        const changeFromBase = newPrice - basePrice;
        const changeRate = parseFloat(((changeFromBase / basePrice) * 100).toFixed(2));
        const volume = Math.floor(Math.random() * 1000) + 10;

        const marketData: MarketData = {  //ts에서 데이터의 규격이 포함됨.
          symbol,   //데이터의 값이 생략된 것으로 "symbol":symbol.
          price: newPrice,
          change: changeFromBase,
          changeRate,
          volume,
          timestamp: Date.now()
        };

        const channel = `market:data:${symbol}`;
        const cacheKey = `market:cache:${symbol}`;
        const payload = JSON.stringify(marketData);

        // 로컬 이벤트 버스에는 항상 전송 (Fallback 대비)
        localEventBus.emit(channel, payload);
        localEventBus.setCache(cacheKey, payload);

        // Redis 연결이 정상일 때만 Redis에도 전송
        if (this.redisClient && !this.useFallback) {
          try {
            await this.redisClient.publish(channel, payload);
            await this.redisClient.set(cacheKey, payload);
          } catch (err) {
            // 에러 시 로깅은 최소화 (이미 error 이벤트에서 fallback 처리)
          }
        }
      }
    }, 500);
  }

  /**
   * 가상 피드 발생기 정지
   */
  public stopFeed() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('Mock Koscom Feed Generator stopped.');
    }
  }

  /**
   * 자원 정리
   */
  public async close() {
    this.stopFeed();
    if (this.redisClient) {
      try {
        await this.redisClient.quit();
      } catch (e) {}
    }
  }
}
