import { WebSocket } from 'ws';
import { CONFIG } from '../config';

export interface ClientSession {
  ws: WebSocket;
  clientId?: string;
  subscriptions: Set<string>; // 'tr_id:symbol' (e.g. 'H0STCNT0:005930')
  connectedAt: Date;
  missedPingCount: number;
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
      missedPingCount: 0,
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
   * 특정 TR + 종목 구독 시도 (구독 키: 'tr_id:symbol', 최대 10개 한도 검증)
   */
  public subscribe(ws: WebSocket, trId: string, symbol: string): { success: boolean; error?: string; currentSubscriptions: string[] } {
    const session = this.sessions.get(ws);
    if (!session) {
      return { success: false, error: 'Session not found', currentSubscriptions: [] };
    }

    const subKey = `${trId}:${symbol}`;

    // 이미 구독 중인지 검사
    if (session.subscriptions.has(subKey)) {
      return {
        success: true,
        currentSubscriptions: Array.from(session.subscriptions)
      };
    }

    // 최대 10개 구독 제한 검증
    if (session.subscriptions.size >= CONFIG.MAX_SUBSCRIPTION_LIMIT) {
      return {
        success: false,
        error: `Subscription limit of ${CONFIG.MAX_SUBSCRIPTION_LIMIT} exceeded.`,
        currentSubscriptions: Array.from(session.subscriptions)
      };
    }

    // 구독 추가
    session.subscriptions.add(subKey);
    return {
      success: true,
      currentSubscriptions: Array.from(session.subscriptions)
    };
  }

  /**
   * 특정 TR + 종목 구독 해제
   */
  public unsubscribe(ws: WebSocket, trId: string, symbol: string): { success: boolean; currentSubscriptions: string[] } {
    const session = this.sessions.get(ws);
    if (!session) {
      return { success: false, currentSubscriptions: [] };
    }

    const subKey = `${trId}:${symbol}`;
    session.subscriptions.delete(subKey);
    return {
      success: true,
      currentSubscriptions: Array.from(session.subscriptions)
    };
  }

  /**
   * 특정 TR + 종목을 구독하고 있는 모든 WebSocket 세션 목록 조회 (Broadcasting용)
   */
  public getSubscribedSessions(trId: string, symbol: string): WebSocket[] {
    const subKey = `${trId}:${symbol}`;
    const subscribedWS: WebSocket[] = [];
    for (const [ws, session] of this.sessions.entries()) {
      if (session.subscriptions.has(subKey)) {
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

  /**
   * 모든 활성 세션 목록 조회
   */
  public getAllSessions(): ClientSession[] {
    return Array.from(this.sessions.values());
  }

  /**
   * 특정 세션의 Ping 미응답 카운트 0으로 초기화
   */
  public resetMissedPing(ws: WebSocket): void {
    const session = this.sessions.get(ws);
    if (session) {
      session.missedPingCount = 0;
    }
  }

  /**
   * 특정 세션의 Ping 미응답 카운트 1 증가
   */
  public incrementMissedPing(ws: WebSocket): number {
    const session = this.sessions.get(ws);
    if (session) {
      session.missedPingCount += 1;
      return session.missedPingCount;
    }
    return 0;
  }
}
