import WebSocket from 'ws';
import { CONFIG } from './config';
import { OpenApiRequestMessage, ServerMessage } from './types';

const BASE_URL = `ws://localhost:${CONFIG.PORT}`;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Custom JSON 요청 생성 헬퍼
function createSubMsg(approvalKey: string, trType: '1' | '2', symbol: string): OpenApiRequestMessage {
  return {
    header: {
      approval_key: approvalKey,
      custtype: 'P',
      tr_type: trType,
      'content-type': 'utf-8'
    },
    body: {
      input: {
        tr_id: 'H0STCNT0',
        tr_key: symbol
      }
    }
  };
}

async function runTestClient() {
  console.log('=== Starting WebSocket Revamped Verification Client (No client_id in URL) ===\n');

  // 1. 잘못된 URI 경로 차단 테스트
  console.log('[Test 1] Attempting connection to invalid path (/check)...');
  await new Promise<void>((resolve) => {
    const wsInvalidPath = new WebSocket(`${BASE_URL}/check`); // client_id 쿼리 파라미터 완전 제외
    
    wsInvalidPath.on('open', () => {
      console.error('❌ [Test 1 Failed] Connected successfully to invalid path!');
      wsInvalidPath.close();
      resolve();
    });

    wsInvalidPath.on('unexpected-response', (req, res) => {
      console.log(`✅ [Test 1 Success] Rejected with status code: ${res.statusCode} (${res.statusMessage})`);
      resolve();
    });

    wsInvalidPath.on('error', () => {
      resolve();
    });
  });

  await sleep(5000);

  // 2. 정상 경로 접속 (/authorize - query parameter 없음)
  console.log('\n[Test 2] Connecting to /authorize without any query parameters...');
  const ws1 = new WebSocket(`${BASE_URL}/authorize`); // 쿼리 없음
  
  await new Promise<void>((resolve) => {
    ws1.on('open', () => {
      console.log('✅ [Test 2 Success] Conn 1: Connected successfully to /authorize without query params.');
      resolve();
    });
  });

  let ws1DisconnectedByServer = false;

  ws1.on('message', (data: string) => {
    const msg: ServerMessage = JSON.parse(data);
    if (msg.type === 'ERROR' && msg.code === 'DUPLICATE_LOGIN') {
      console.log(`Conn 1 Received: ⚠️ ERROR (${msg.code}) - ${msg.message}`);
      ws1DisconnectedByServer = true;
    }
  });

  ws1.on('close', () => {
    console.log('Conn 1: Connection closed.');
  });

  // 3. Conn 1에서 approval_key: "test"로 첫 번째 구독 신청 (서버가 세션 소유자를 "test"로 바인딩)
  console.log('\nConn 1: Sending first subscription request for "005930" with approval_key: "test"...');
  const initMsg = createSubMsg('test', '1', '005930');
  ws1.send(JSON.stringify(initMsg));
  await sleep(500);

  // 4. 중복 세션 제어 테스트 (동일한 approval_key: "test"로 Conn 2 생성 및 구독 시도)
  console.log('\n[Test 3] Testing Single Session policy via approval_key...');
  const ws2 = new WebSocket(`${BASE_URL}/authorize`);
  
  await new Promise<void>((resolve) => {
    ws2.on('open', () => {
      console.log('Conn 2: Connected successfully.');
      resolve();
    });
  });

  // Conn 2에서 동일한 approval_key: "test" 로 구독 시도
  console.log('Conn 2: Sending subscription request with SAME approval_key: "test"...');
  const dupMsg = createSubMsg('test', '1', '000660');
  ws2.send(JSON.stringify(dupMsg));
  
  await sleep(1000); // ws1의 Disconnect 처리 시간 대기

  if (ws1DisconnectedByServer) {
    console.log('✅ [Test 3 Success] Conn 1 was kicked out due to duplicate approval_key in Conn 2.');
  } else {
    console.error('❌ [Test 3 Failed] Conn 1 was NOT kicked out.');
  }

  // 5. Custom JSON 및 10개 한도 제한 테스트 (Conn 2 이용)
  console.log('\n[Test 4] Testing 10-symbol subscription limit with custom JSON...');
  
  ws2.on('message', (data: string) => {
    try {
      const message: ServerMessage = JSON.parse(data);
      switch (message.type) {
        case 'SUBSCRIBED':
          console.log(`✅ [Response] SUBSCRIBED to ${message.symbol}. Active: ${message.currentSubscriptions.length} (${message.currentSubscriptions.join(', ')})`);
          break;
        case 'UNSUBSCRIBED':
          console.log(`ℹ️ [Response] UNSUBSCRIBED from ${message.symbol}. Active: ${message.currentSubscriptions.length} (${message.currentSubscriptions.join(', ')})`);
          break;
        case 'MARKET_DATA':
          console.log(`📈 [Data] Symbol: ${message.symbol} | Price: ${message.data.price} | Change: ${message.data.change} (${message.data.changeRate}%) | Vol: ${message.data.volume}`);
          break;
        case 'ERROR':
          console.log(`❌ [Error Response] Code: ${message.code} | Message: ${message.message}`);
          break;
      }
    } catch (e) {
      console.error('Failed to parse response:', data);
    }
  });

  // 5-1. 잘못된 approval_key로 요청하여 실패하는지 검증
  console.log('Sending subscription with WRONG approval_key...');
  const wrongKeyMsg = createSubMsg('wrong_key', '1', CONFIG.MOCK_SYMBOLS[0]);
  ws2.send(JSON.stringify(wrongKeyMsg));
  await sleep(500);

  // 5-2. 정상 approval_key("test")로 12개 종목 구독 시도 (최대 10개 한도 제한 테스트)
  console.log('\nSending subscription for 12 symbols sequentially with approval_key: "test"...');
  const testSymbols = CONFIG.MOCK_SYMBOLS.slice(0, 12);
  
  for (const symbol of testSymbols) {
    const subMsg = createSubMsg('test', '1', symbol);
    ws2.send(JSON.stringify(subMsg));
    await sleep(200); // 지연
  }

  // 6. 4초간 실시간 데이터 관찰
  console.log('\n--- Observing Real-time Market Data for 4 seconds ---');
  await sleep(4000);

  // 7. 구독 해제 및 신규 등록 테스트
  const firstSymbol = testSymbols[0];
  const failedSymbol = testSymbols[10]; // 11번째 종목

  console.log(`\n[Test 5] Unsubscribing (tr_type: "2") from: ${firstSymbol}`);
  const unsubMsg = createSubMsg('test', '2', firstSymbol);
  ws2.send(JSON.stringify(unsubMsg));
  await sleep(500);

  console.log(`[Test 5] Subscribing (tr_type: "1") to previously failed symbol: ${failedSymbol}`);
  const retryMsg = createSubMsg('test', '1', failedSymbol);
  ws2.send(JSON.stringify(retryMsg));
  await sleep(1000);

  // 3초간 추가 관찰 후 종료
  console.log('\n--- Final observation for 3 seconds before disconnect ---');
  await sleep(3000);

  console.log('Closing client connection...');
  ws2.close();
}

runTestClient().catch((err) => {
  console.error('Test client run error:', err);
});
