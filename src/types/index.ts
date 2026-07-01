// OpenAPI Custom Message Format
export interface OpenApiRequestHeader {
  approval_key: string;
  custtype: string;
  tr_type: '1' | '2'; // '1': Register/Subscribe, '2': Unsubscribe
  'content-type': string;
}

export interface OpenApiRequestBody {
  input: {
    tr_id: string; // e.g., 'H0STCNT0'
    tr_key: string; // Symbol (e.g., '005930')
  };
}

export interface OpenApiRequestMessage {
  header: OpenApiRequestHeader;
  body: OpenApiRequestBody;
}

// ClientMessage is now alias to OpenApiRequestMessage
export type ClientMessage = OpenApiRequestMessage;

// Server response structure (Matches similar style or keeps clean JSON)
export type ServerMessageType = 'SUBSCRIBED' | 'UNSUBSCRIBED' | 'MARKET_DATA' | 'ERROR';

export interface BaseServerResponse {
  type: ServerMessageType;
}

export interface SubscriptionResponse extends BaseServerResponse {
  type: 'SUBSCRIBED' | 'UNSUBSCRIBED';
  symbol: string;
  currentSubscriptions: string[];
}

export interface MarketDataMessage extends BaseServerResponse {
  type: 'MARKET_DATA';
  symbol: string;
  data: MarketData;
}

export interface ErrorMessage extends BaseServerResponse {
  type: 'ERROR';
  message: string;
  code: string;
}

export type ServerMessage = SubscriptionResponse | MarketDataMessage | ErrorMessage;

// Market Data Schema
export interface MarketData {
  symbol: string;
  price: number;
  change: number;
  changeRate: number;
  volume: number;
  timestamp: number;
}
