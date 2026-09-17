import fs from 'fs';
import path from 'path';

export interface SymbolInfo {
  symbol: string;
  name: string;
}

export class SymbolManager {
  private static symbolSet: Set<string> = new Set();
  private static symbolMap: Map<string, SymbolInfo> = new Map();
  private static isLoaded: boolean = false;

  /**
   * 종목 파일(JSON)을 읽어 메모리에 로드
   */
  public static loadSymbols(filePath?: string): void {
    const defaultPath = path.resolve(__dirname, '../../data/symbols.json');
    const targetPath = filePath || defaultPath;

    try {
      console.log(`[SymbolManager] Loading valid symbols from file: ${targetPath}`);
      const rawData = fs.readFileSync(targetPath, 'utf-8');
      const symbols: SymbolInfo[] = JSON.parse(rawData);

      this.symbolSet.clear();
      this.symbolMap.clear();

      symbols.forEach((item) => {
        if (item.symbol) {
          this.symbolSet.add(item.symbol);
          this.symbolMap.set(item.symbol, item);
        }
      });

      this.isLoaded = true;
      console.log(`✅ [SymbolManager] Loaded ${this.symbolSet.size} valid symbols into memory.`);
    } catch (err: any) {
      console.error(`❌ [SymbolManager] Failed to load symbols file: ${err.message}`);
      throw err;
    }
  }

  /**
   * 유효한 종목코드인지 검증 (O(1))
   */
  public static isValidSymbol(symbol: string): boolean {
    if (!this.isLoaded) {
      this.loadSymbols();
    }
    return this.symbolSet.has(symbol);
  }

  /**
   * 전체 유효 종목코드 목록 반환
   */
  public static getValidSymbols(): string[] {
    if (!this.isLoaded) {
      this.loadSymbols();
    }
    return Array.from(this.symbolSet);
  }

  /**
   * 특정 종목 상세정보 반환
   */
  public static getSymbolInfo(symbol: string): SymbolInfo | undefined {
    if (!this.isLoaded) {
      this.loadSymbols();
    }
    return this.symbolMap.get(symbol);
  }
}
