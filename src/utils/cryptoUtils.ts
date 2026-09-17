import crypto from 'crypto';

const DEFAULT_SECRET_KEY = process.env.ENCRYPTION_KEY || 'market-server-secret-key-32bytes!';

/**
 * 평문 비밀번호를 AES-256-CBC 로 암호화하여 ENC(iv:cipher) 포맷으로 변환
 */
export function encryptPassword(plainText: string, secretKey: string = DEFAULT_SECRET_KEY): string {
  const key = Buffer.from(secretKey.padEnd(32).slice(0, 32));
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);

  let encrypted = cipher.update(plainText, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  const ivHex = iv.toString('hex');
  return `ENC(${ivHex}:${encrypted})`;
}

/**
 * ENC(iv:cipher) 형식의 암호문을 복호화하여 평문 비밀번호 반환
 */
export function decryptPassword(value?: string, secretKey: string = DEFAULT_SECRET_KEY): string | undefined {
  if (!value) return value;
  if (!value.startsWith('ENC(') || !value.endsWith(')')) return value;

  const inner = value.slice(4, -1);
  const parts = inner.split(':');
  if (parts.length !== 2) return value;

  const [ivHex, encryptedHex] = parts;
  if (!ivHex || !encryptedHex) return value;

  try {
    const key = Buffer.from(secretKey.padEnd(32).slice(0, 32));
    const iv = Buffer.from(ivHex, 'hex');
    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);

    let decrypted = decipher.update(encryptedHex, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch (err) {
    console.error('⚠️ [Crypto] Failed to decrypt password:', (err as Error).message);
    return value;
  }
}
