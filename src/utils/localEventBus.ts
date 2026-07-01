import { EventEmitter } from 'events';

class LocalEventBus extends EventEmitter {
  private cache: Map<string, string> = new Map();

  public setCache(key: string, value: string) {
    this.cache.set(key, value);
  }

  public getCache(key: string): string | null {
    return this.cache.get(key) || null;
  }
}

export const localEventBus = new LocalEventBus();
// Redis가 없거나 오류 시 fallback으로 사용하기 위해 MaxListeners 상향 조정
localEventBus.setMaxListeners(100);
