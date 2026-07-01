import { WebSocket } from 'ws';
import { CONFIG } from '../config';

export interface ClientSession {
  ws: WebSocket;
  clientId?: string; // 옵셔널 필드로 변환 (최초 연결 시엔 할당되지 않음)
  subscriptions: Set<string>;
  connectedAt: Date;
}

export class SessionManager {
  private sessions: Map<WebSocket, ClientSession> = new Map();

  /**
   * 새로운 WebSocket 연결 세션 추가
   */
  public addSession(ws: WebSocket): ClientSession {
    const session: ClientSession = {
      ws,
      subscriptions: new Set<string>(),
      connectedAt: new Date(),
    };
    this.sessions.set(ws, session);
    return session;
  }

  /**
   * WebSocket 연결 세션 제거
   */
  public removeSession(ws: WebSocket): ClientSession | undefined {
    const session = this.sessions.get(ws);
    if (session) {
      this.sessions.delete(ws);
    }
    return session;
  }

  /**
   * 세션 가져오기
   */
  public getSession(ws: WebSocket): ClientSession | undefined {
    return this.sessions.get(ws);
  }

  /**
   * 세션에 Client ID 바인딩 (최초 구독 요청 시 사용)
   */
  public bindClientId(ws: WebSocket, clientId: string): boolean {
    const session = this.sessions.get(ws);
    if (session) {
      session.clientId = clientId;
      return true;
    }
    return false;
  }

  /**
   * Client ID로 이미 활성화된 세션이 있는지 조회 (1 ID 1 Session 검증용)
   */
  public getSessionByClientId(clientId: string): ClientSession | undefined {
    for (const session of this.sessions.values()) {
      if (session.clientId === clientId) {
        return session;
      }
    }
    return undefined;
  }

  /**
   * 특정 종목 구독 시도 (최대 10개 한도 검증)
   */
  public subscribe(ws: WebSocket, symbol: string): { success: boolean; error?: string; currentSubscriptions: string[] } {
    const session = this.sessions.get(ws);
    if (!session) {
      return { success: false, error: 'Session not found', currentSubscriptions: [] };
    }

    // 이미 구독 중인지 검사
    if (session.subscriptions.has(symbol)) {
      return { 
        success: true, 
        currentSubscriptions: Array.from(session.subscriptions) 
      };
    }

    // 최대 10개 구독 제한 검증
    if (session.subscriptions.size >= CONFIG.MAX_SUBSCRIPTION_LIMIT) {
      return {
        success: false,
        error: `Subscription limit of ${CONFIG.MAX_SUBSCRIPTION_LIMIT} symbols exceeded.`,
        currentSubscriptions: Array.from(session.subscriptions)
      };
    }

    // 구독 추가
    session.subscriptions.add(symbol);
    return {
      success: true,
      currentSubscriptions: Array.from(session.subscriptions)
    };
  }

  /**
   * 특정 종목 구독 해제
   */
  public unsubscribe(ws: WebSocket, symbol: string): { success: boolean; currentSubscriptions: string[] } {
    const session = this.sessions.get(ws);
    if (!session) {
      return { success: false, currentSubscriptions: [] };
    }

    session.subscriptions.delete(symbol);
    return {
      success: true,
      currentSubscriptions: Array.from(session.subscriptions)
    };
  }

  /**
   * 특정 종목을 구독하고 있는 모든 WebSocket 세션 목록 조회 (Broadcasting용)
   */
  public getSubscribedSessions(symbol: string): WebSocket[] {
    const subscribedWS: WebSocket[] = [];
    for (const [ws, session] of this.sessions.entries()) {
      if (session.subscriptions.has(symbol)) {
        subscribedWS.push(ws);
      }
    }
    return subscribedWS;
  }

  /**
   * 전체 세션 개수 조회
   */
  public getSessionCount(): number {
    return this.sessions.size;
  }
}
