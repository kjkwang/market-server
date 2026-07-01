import { KoscomConnector } from './gateway/koscomConnector';
import { WsServer } from './websocket/wsServer';

async function main() {
  console.log('=== Initializing Koscom Market Data WebSocket System ===');

  // 1. Koscom Feed Connector (Gateway) 구동 및 시뮬레이션 데이터 발생 시작
  const koscomConnector = new KoscomConnector();
  koscomConnector.startFeed();

  // 2. WebSocket Server 구동
  const wsServer = new WsServer();
  await wsServer.start();

  console.log('System is fully operational.');

  // Graceful Shutdown 처리
  const gracefulShutdown = async (signal: string) => {
    console.log(`\nReceived ${signal}. Shutting down gracefully...`);
    
    try {
      await wsServer.close();
      await koscomConnector.close();
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
