// OpenAPI Custom Message Format
export interface OpenApiRequestHeader {
  approval_key: string;
  custtype: string;
  tr_type: '1' | '2'; // '1': Register/Subscribe, '2': Unsubscribe
  'content-type': string;
}

export interface OpenApiRequestBody {
  input: {
    tr_id: string; // e.g., 'H0STCNT0' (체결가) or 'H0STASP0' (호가)
    tr_key: string; // Symbol (e.g., '005930')
  };
}

export interface OpenApiRequestMessage {
  header: OpenApiRequestHeader;
  body: OpenApiRequestBody;
}

// ClientMessage is alias to OpenApiRequestMessage
export type ClientMessage = OpenApiRequestMessage;

// Server response structure
export type ServerMessageType = 'SUBSCRIBED' | 'UNSUBSCRIBED' | 'MARKET_DATA' | 'ERROR';

export interface BaseServerResponse {
  type: ServerMessageType;
}

export interface OpenApiResponseHeader {
  tr_id: string;
  tr_key: string;
  encrypt: string;
}

export interface OpenResponseBody {
  rt_cd: string;
  msg_cd: string;
  msg1: string;
  output?: {
    iv: string;
    key: string;
  };
}

export interface SubscriptionResponse extends BaseServerResponse {
  type: 'SUBSCRIBED' | 'UNSUBSCRIBED';
  tr_id: string;
  symbol: string;
  currentSubscriptions: string[];
  header?: OpenApiResponseHeader;
  body?: OpenResponseBody;
}

export interface MarketDataMessage extends BaseServerResponse {
  type: 'MARKET_DATA';
  tr_id: string;
  symbol: string;
  data: MarketData;
}

export interface ErrorMessage extends BaseServerResponse {
  type: 'ERROR';
  message: string;
  code: string;
}

export type ServerMessage = SubscriptionResponse | MarketDataMessage | ErrorMessage;

// 1. 실시간 체결가 데이터 (H0STCNT0)
export interface TradeData {
  tr_id: 'H0STCNT0';
  symbol: string;
  price: number;
  change: number;
  changeRate: number;
  volume: number;
  timestamp: number;
}

// 2. 실시간 호가 데이터 (H0STASP0)
export interface OrderbookItem {
  price: number;
  volume: number;
}

export interface OrderbookData {
  tr_id: 'H0STASP0';
  symbol: string;
  ask: OrderbookItem[]; // 매도 1~10 호가 및 잔량
  bid: OrderbookItem[]; // 매수 1~10 호가 및 잔량
  totalAskVolume: number;
  totalBidVolume: number;
  timestamp: number;
}

export type MarketData = TradeData | OrderbookData;
