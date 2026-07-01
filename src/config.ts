export const CONFIG = {
  PORT: parseInt(process.env.PORT || '8088', 10),
  WS_PATH: '/authorize',
  REDIS_URL: process.env.REDIS_URL || 'redis://192.168.0.171:6379',
  MAX_SUBSCRIPTION_LIMIT: 10,
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
