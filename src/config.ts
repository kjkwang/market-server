import { RedisOptions } from 'ioredis';

export const REDIS_CONFIG: RedisOptions = {
  sentinels: [
    { host: process.env.REDIS_SENTINEL_HOST_1 || '192.168.0.172', port: Number(process.env.REDIS_SENTINEL_PORT_1) || 26379 },
    { host: process.env.REDIS_SENTINEL_HOST_2 || '192.168.0.173', port: Number(process.env.REDIS_SENTINEL_PORT_2) || 26379 },
    { host: process.env.REDIS_SENTINEL_HOST_3 || '192.168.0.174', port: Number(process.env.REDIS_SENTINEL_PORT_3) || 26379 },
  ],
  name: process.env.REDIS_MASTER_NAME || 'mymaster',
  role: 'master',
  // 🔥 [변경] 센티널들이 살아있는 주소 목록을 주기적으로 동기화하도록 켭니다.
  updateSentinels: true,
  failoverDetector: true,
  //natAsExternalIp: true,
  //sentinelConnectTimeout: 10000, 
  sentinelCommandTimeout: 10000,
  maxRetriesPerRequest: null,
  retryStrategy: (times: number) => Math.min(times * 500, 5000),
  sentinelRetryStrategy: (times: number) => {
    console.log(`♻️ [Redis Config] 센티널 재접속 시도 횟수: ${times}회째...`);
    
    // 끊임없이 접속을 시도하게 하려면 숫자를 리턴하고, 특정 횟수에서 포기하게 하려면 null을 리턴하면 됩니다.
    // 여기서는 실패할 때마다 최대 5초(5000ms) 간격으로 무한히 대기하며 접속을 끈질기게 시도합니다.
    return Math.min(times * 500, 5000); 
  },
  //sentinelRetryStrategy: (times: number) => Math.min(times * 500, 5000),
  reconnectOnError: (err: Error) => {
    const targetError = 'READONLY';
    if (err.message.includes(targetError)) {
      console.warn('⚠️ [Redis] READONLY error encountered. Reconnecting to new master...');
      return 2; // Reconnect and resend command
    }
    return false;
  },
  ...(process.env.REDIS_PASSWORD ? { password: process.env.REDIS_PASSWORD } : {}),
  ...(process.env.REDIS_SENTINEL_PASSWORD ? { sentinelPassword: process.env.REDIS_SENTINEL_PASSWORD } : {}),
};

export const CONFIG = {
  PORT: 8088,
  API_PORT: 8089,
  WS_PATH: '/tryitout',
  REDIS_URL: 'redis://192.168.0.172:6379',
  MAX_SUBSCRIPTION_LIMIT: 10,
  PINGPONG_INTERVAL_MS: 20000,
  PINGPONG_MAX_MISS_COUNT: 3,
  MOCK_SYMBOLS: [
    '005930', // 삼성전자
    '000660', // SK하이닉스
    '035420', // NAVER
    '035720', // 카카오
    '005380', // 현대차
    '051910', // LG화학
    '006400', // 삼성SDI
    '207940', // 삼성바이오로직스
    '068270', // 셀트리온
    '000270', // 기아
    '012330', // 현대모비스
    '066570', // LG전자
    '032830', // 삼성생명
    '003550', // LG
    '015760', // 한국전력
  ]
};

