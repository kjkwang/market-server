

국내주식 WebSocket
구분	TR ID	내용
실시간 체결가	H0STCNT0	주식이 실제로 체결될 때 발생
실시간 호가	H0STASP0	매도/매수 1~10호가와 잔량
실시간 체결통보	H0STCNI0	본인 계좌의 주문 체결 통보

1. 실시간 체결가 — H0STCNT0
예를 들어 삼성전자 005930을 등록하면 체결이 발생할 때마다 데이터가 들어옵니다.
주요 데이터:
현재 체결가격
체결시간
전일 대비
등락률
체결수량
누적거래량
시가
고가
저가
매도/매수 체결 관련 정보
거래대금
VI 관련 정보 등

2. 실시간 호가 — H0STASP0
이건 호가창 데이터입니다.
매도10호가
...
매도2호가
매도1호가
----------------
매수1호가
매수2호가
...
매수10호가
가격뿐 아니라 각 호가별 잔량도 실시간으로 전달됩니다. 공식 코드에서는 ASKP1~10, BIDP1~10, 각 호가 잔량 등을 제공합니다

실시간 체결통보 — H0STCNI0
이건 시장 전체의 체결 데이터가 아닙니다.
내 계좌에서 주문한 주식이 체결됐을 때
주문
 ↓
체결
 ↓
H0STCNI0
 ↓
체결통보
형태로 들어오는 계좌 전용 WebSocket 데이터입니다. 공식 샘플에서도 H0STCNI0을 고객용 주식 체결통보로 사용합니다.

HDFSASP0  → 해외주식 실시간 호가
HDFSCNT0  → 해외주식 실시간 체결
H0GSCNI0  → 해외주식 실시간 체결통보

HDFSASP0  → 해외주식 실시간 호가
HDFSCNT0  → 해외주식 실시간 체결



H0GSCNI0  → 해외주식 실시간 체결통보

H0STCNT0 실시간 체결가		주식이 실제로 체결될 때 발생
H0STASP0	실시간 호가	 매도/매수 1~10호가와 잔량

A. Redis Pub/Sub 채널 & 캐시 키 채용 규칙
Pub/Sub 채널: market:data:{tr_id}:{symbol}
예: market:data:H0STCNT0:005930 (실시간 체결가 피드)
예: market:data:H0STASP0:005930 (실시간 호가 피드)
Redis 캐시 키: market:cache:{tr_id}:{symbol}
예: market:cache:H0STCNT0:005930
예: 


H0STCNT0 (실시간 체결가)


{
  "tr_id": "H0STCNT0",
  "symbol": "005930",
  "price": 75300,
  "change": 300,
  "changeRate": 0.40,
  "volume": 412,
  "timestamp": 1771924150000
}

H0STASP0 (실시간 1~10 호가)

json


{
  "tr_id": "H0STASP0",
  "symbol": "005930",
  "ask": [
    { "price": 75400, "volume": 1200 },
    { "price": 75500, "volume": 3500 },
    ... (10호가까지)
  ],
  "bid": [
    { "price": 75300, "volume": 2100 },
    { "price": 75200, "volume": 4200 },
    ... (10호가까지)
  ],
  "totalAskVolume": 34500,
  "totalBidVolume": 41200,
  "timestamp": 1771924150000
}


{
  "header": {
    "approval_key": "test",
    "custtype": "P",
    "tr_type": "1",
    "content-type": "utf-8"
  },
  "body": {
    "input": {
      "tr_id": "H0STASP0",
      "tr_key": "005930"
    }
  }
}

D. REST API 호환 업데이트 (
src/api/apiServer.ts
)
특정 TR 전체 종목 시세: GET http://localhost:8089/api/market/cache/H0STCNT0
특정 TR 특정 종목 시세: GET http://localhost:8089/api/market/cache/H0STASP0/005930