import { InfraSiseConnector } from './gateway/infraSiseConnector';
import { WsServer } from './websocket/wsServer';
import { ApiServer } from './api/apiServer';
import { SymbolManager } from './utils/symbolManager';






async function main() {
  console.log('=== Initializing InfraSise Market Data WebSocket System ===');

  // 0. 유효 종목 데이터 파일(data/symbols.json)을 메모리에 로드
  SymbolManager.loadSymbols();

  // 1. InfraSise Feed Connector (Gateway) 구동 및 시뮬레이션 데이터 발생 시작
  const infraSiseConnector = new InfraSiseConnector();
  infraSiseConnector.startFeed();

  // 2. WebSocket Server 구동
  const wsServer = new WsServer();
  await wsServer.start();

  // 3. HTTP API Server 구동
  const apiServer = new ApiServer();
  apiServer.start();

  console.log('System is fully operational.');

  // Graceful Shutdown 처리
  const gracefulShutdown = async (signal: string) => {
    console.log(`\nReceived ${signal}. Shutting down gracefully...`);

    try {
      await wsServer.close();
      await infraSiseConnector.close();
      await apiServer.close();
      console.log('Shutdown complete. Goodbye!');
      process.exit(0);
    } catch (err) {
      console.error('Error during shutdown:', err);
      process.exit(1);
    }
  };

  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal initialization error:', err);
  process.exit(1);
});
