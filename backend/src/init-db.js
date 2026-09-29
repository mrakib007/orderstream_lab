import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { pool } from './db.js';

try {
  const schemaPath = fileURLToPath(new URL('./schema.sql', import.meta.url));
  const schema = await readFile(schemaPath, 'utf8');
  await pool.query(schema);
  console.log('Database schema is ready: catalog and learning tables created or already present.');
} catch (error) {
  console.error('Could not initialize PostgreSQL. Check backend/.env and that the orderstream_lab database exists.');
  console.error(error.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
