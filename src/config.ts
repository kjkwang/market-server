import 'dotenv/config';
import { RedisOptions } from 'ioredis';
import { decryptPassword } from './utils/cryptoUtils';

// Redis 역할 선택 설정값 ('master' 또는 'slave' / 'replica')
// ioredis Sentinel 모드에서 read 전용 작업을 replica(slave) 노드로 분기하기 위한 설정값
const rawRole = (process.env.REDIS_ROLE || 'master').toLowerCase();
export const REDIS_ROLE: 'master' | 'slave' = (rawRole === 'replica' || rawRole === 'slave') ? 'slave' : 'master';

// REDIS_READ_ROLE 환경변수가 명시되지 않으면 기본값은 'master'로 동작하여 Replica 미구성 환경에서의 Sentinel 접속 에러를 방지합니다.
// Replica 노드로 읽기 분기를 수행하려면 환경변수 REDIS_READ_ROLE=replica (또는 slave)를 명시합니다.
const rawReadRole = process.env.REDIS_READ_ROLE ? process.env.REDIS_READ_ROLE.toLowerCase() : 'master';
export const REDIS_READ_ROLE: 'master' | 'slave' = (rawReadRole === 'replica' || rawReadRole === 'slave') ? 'slave' : 'master';

export const REDIS_CONFIG: RedisOptions = {
  sentinels: [
    { host: process.env.REDIS_SENTINEL_HOST_1 || '192.168.0.172', port: Number(process.env.REDIS_SENTINEL_PORT_1) || 26379 },
    { host: process.env.REDIS_SENTINEL_HOST_2 || '192.168.0.173', port: Number(process.env.REDIS_SENTINEL_PORT_2) || 26379 },
    { host: process.env.REDIS_SENTINEL_HOST_3 || '192.168.0.174', port: Number(process.env.REDIS_SENTINEL_PORT_3) || 26379 },
  ],
  name: process.env.REDIS_MASTER_NAME || 'mymaster',
  //role: REDIS_ROLE,
  
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
  ...(process.env.REDIS_PASSWORD ? { password: decryptPassword(process.env.REDIS_PASSWORD) } : {}),
  ...(process.env.REDIS_SENTINEL_PASSWORD ? { sentinelPassword: decryptPassword(process.env.REDIS_SENTINEL_PASSWORD) } : {}),
};

// 읽기 전용 작업(Replica/Slave)을 위한 Redis Config
export const REDIS_READ_CONFIG: RedisOptions = {
  ...REDIS_CONFIG,
  role: REDIS_READ_ROLE,
};


// WebSocket 서버 고유 ID (환경 변수가 없거나 비어 있으면 ws-server + 5자리 난수 조합으로 자동 생성)
const defaultRandomId = Math.random().toString(36).substring(2, 7);
export const SERVER_ID = (process.env.SERVER_ID && process.env.SERVER_ID.trim() !== '') 
  ? process.env.SERVER_ID.trim() 
  : `ws-server-${defaultRandomId}`;

export const CONFIG = {
  SERVER_ID,
  PORT: parseInt(process.env.PORT || '8088', 10),
  API_PORT: parseInt(process.env.API_PORT || '8089', 10),
  WS_PATH: '/tryitout',
  REDIS_URL: process.env.REDIS_URL || 'redis://192.168.0.172:6379',
  MAX_SUBSCRIPTION_LIMIT: 10,
  PINGPONG_INTERVAL_MS: 20000,
  PINGPONG_MAX_MISS_COUNT: 3,
  // 계정당 다중 접속 허용 여부 (false: 1계정 1연결 단일 세션 제한, true: 다중 접속 허용)
  ALLOW_MULTIPLE_CONNECTIONS: process.env.ALLOW_MULTIPLE_CONNECTIONS === 'true',
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

