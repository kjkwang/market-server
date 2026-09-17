import asyncio
import json
import websockets

# WebSocket 서버 설정 (CONFIG.PORT = 8088, WS_PATH = '/authorize')
SERVER_URL = "ws://localhost:8088/authorize"

# 모의 주식 종목 코드 리스트 (테스트용 12개)
MOCK_SYMBOLS = [
    '005930', '000660', '035420', '035720', '005380',
    '051910', '006400', '207940', '068270', '000270',
    '012330', '066570'
]

def create_sub_message(approval_key: str, tr_type: str, symbol: str) -> dict:
    """한투/코스콤 스타일의 구독/구독해제 JSON 메시지 생성"""
    return {
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": trType if (trType := tr_type) else "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": "H0STCNT0",
                "tr_key": symbol
            }
        }
    }

async def receive_messages(websocket):
    """서버로부터 실시간 메시지를 수신하여 화면에 출력하는 비동기 루프"""
    try:
        async for raw_message in websocket:
            message = json.loads(raw_message)
            msg_type = message.get("type")
            body = message.get("body", {})
            header = message.get("header", {})

            if isinstance(body, dict) and body.get("msg1"):
                print(f"✅ [Response] {body.get('msg1')} | tr_id: {header.get('tr_id')} | tr_key: {header.get('tr_key')} | key: {body.get('output', {}).get('key')}")

            elif msg_type == "SUBSCRIBED":
                symbols_str = ", ".join(message.get("currentSubscriptions", []))
                print(f"✅ [Response] SUBSCRIBED to {message.get('symbol')}. Active: {len(message.get('currentSubscriptions', []))} ({symbols_str})")
            
            elif msg_type == "UNSUBSCRIBED":
                symbols_str = ", ".join(message.get("currentSubscriptions", []))
                print(f"ℹ️ [Response] UNSUBSCRIBED from {message.get('symbol')}. Active: {len(message.get('currentSubscriptions', []))} ({symbols_str})")
            
            elif msg_type == "MARKET_DATA":
                data = message.get("data", {})
                print(f"📈 [Data] Symbol: {message.get('symbol')} | Price: {data.get('price')} | Change: {data.get('change')} ({data.get('changeRate')}%) | Vol: {data.get('volume')}")
            
            elif msg_type == "ERROR":
                print(f"❌ [Error Response] Code: {message.get('code')} | Message: {message.get('message')}")
                
    except websockets.exceptions.ConnectionClosed:
        print("🔌 Connection closed by server.")
    except Exception as e:
        print(f"⚠️ Error receiving message: {e}")

async def main():
    print("=== Starting Python WebSocket Verification Client (No client_id in URL) ===\n")

    # 1. 잘못된 URI 경로 차단 테스트 (/check)
    print("[Test 1] Attempting connection to invalid path (/check)...")
    try:
        async with websockets.connect(f"ws://localhost:8088/check") as ws_invalid:
            print("❌ [Test 1 Failed] Connected successfully to invalid path!")
    except websockets.exceptions.InvalidStatusCode as e:
        print(f"✅ [Test 1 Success] Rejected with status code: {e.status_code}")
    except Exception as e:
        print(f"✅ [Test 1 Success] Rejected properly: {e}")

    await asyncio.sleep(0.5)

    # 2. 정상 경로 접속 및 중복 세션 제어 테스트 (Conn 1 기동)
    print("\n[Test 2] Connecting Conn 1 to /authorize...")
    try:
        conn1 = await websockets.connect(SERVER_URL)
        print("✅ [Test 2 Success] Conn 1 connected successfully.")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    # Conn 1 수신 태스크 백그라운드 구동
    conn1_recv_task = asyncio.create_task(receive_messages(conn1))

    # Conn 1 세션 바인딩을 위한 최초 구독 신청 (approval_key: "test")
    print("\nConn 1: Sending first subscription request for '005930' with approval_key: 'test'...")
    init_msg = create_sub_message("test", "1", "005930")
    await conn1.send(json.dumps(init_msg))
    await asyncio.sleep(0.5)

    # 3. 중복 로그인 차단 검증 (동일한 approval_key: "test"로 Conn 2 기동 및 구독)
    print("\n[Test 3] Testing Single Session policy by connecting Conn 2...")
    try:
        conn2 = await websockets.connect(SERVER_URL)
        print("Conn 2 connected successfully.")
    except Exception as e:
        print(f"❌ Conn 2 connection failed: {e}")
        conn1_recv_task.cancel()
        await conn1.close()
        return

    conn2_recv_task = asyncio.create_task(receive_messages(conn2))

    # Conn 2에서 동일 키로 구독 요청하여 Conn 1이 튕겨나가는지 관찰
    print("Conn 2: Sending subscription request with SAME approval_key: 'test'...")
    dup_msg = create_sub_message("test", "1", "000660")
    await conn2.send(json.dumps(dup_msg))
    
    await asyncio.sleep(1.5) # Conn 1이 강제 차단되는 시간 대기

    # 4. Custom JSON 및 10개 한도 제한 테스트 (Conn 2 이용)
    print("\n[Test 4] Testing 10-symbol subscription limit with custom JSON...")
    
    # 4-1. 잘못된 approval_key 검증 차단 테스트
    print("Sending subscription with WRONG approval_key...")
    wrong_key_msg = create_sub_message("wrong_key", "1", MOCK_SYMBOLS[0])
    await conn2.send(json.dumps(wrong_key_msg))
    await asyncio.sleep(0.5)

    # 4-2. 정상 approval_key("test")로 12개 종목 구독 시도 (최대 10개 한도 제한 테스트)
    print("\nSending subscription for 12 symbols sequentially with approval_key: 'test'...")
    for symbol in MOCK_SYMBOLS:
        sub_msg = create_sub_message("test", "1", symbol)
        await conn2.send(json.dumps(sub_msg))
        await asyncio.sleep(0.2)

    # 5. 4초간 실시간 가격 변동 수신 관찰
    print("\n--- Observing Real-time Market Data for 4 seconds ---")
    await asyncio.sleep(4.0)

    # 6. 구독 해제 및 신규 등록 테스트
    first_symbol = MOCK_SYMBOLS[0]   # '005930'
    failed_symbol = MOCK_SYMBOLS[10] # '012330'

    print(f"\n[Test 5] Unsubscribing (tr_type: '2') from: {first_symbol}")
    unsub_msg = create_sub_message("test", "2", first_symbol)
    await conn2.send(json.dumps(unsub_msg))
    await asyncio.sleep(0.5)

    print(f"[Test 5] Subscribing (tr_type: '1') to previously failed symbol: {failed_symbol}")
    retry_msg = create_sub_message("test", "1", failed_symbol)
    await conn2.send(json.dumps(retry_msg))
    await asyncio.sleep(1.0)

    # 3초간 최종 시세 수신 관찰
    print("\n--- Final observation for 3 seconds before disconnect ---")
    await asyncio.sleep(3.0)

    print("\nClosing client connection...")
    conn2_recv_task.cancel()
    conn1_recv_task.cancel()
    await conn2.close()
    try:
        await conn1.close()
    except Exception:
        pass
    print("Test finished successfully.")

if __name__ == "__main__":
    asyncio.run(main())
