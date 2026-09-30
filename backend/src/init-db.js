import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { pool } from './db.js';

try {
  for (const fileName of ['./schema.sql', './sports-schema.sql']) {
    const schemaPath = fileURLToPath(new URL(fileName, import.meta.url));
    const schema = await readFile(schemaPath, 'utf8');
    await pool.query(schema);
  }
  console.log('Database schema is ready: OrderStream and sports learning tables created or already present.');
} catch (error) {
  console.error('Could not initialize PostgreSQL. Check backend/.env and that the orderstream_lab database exists.');
  console.error(error.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
