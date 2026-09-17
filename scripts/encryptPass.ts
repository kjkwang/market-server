import 'dotenv/config';
import { encryptPassword } from '../src/utils/cryptoUtils';

const plainPassword = process.argv[2];

if (!plainPassword) {
  console.log('Usage: npx ts-node scripts/encryptPass.ts <plain_password>');
  process.exit(1);
}

const encrypted = encryptPassword(plainPassword);
console.log('\n==========================================');
console.log(`🔑 Original Plain Password : ${plainPassword}`);
console.log(`🔒 Encrypted Value for .env :`);
console.log(`REDIS_PASSWORD=${encrypted}`);
console.log('==========================================\n');
