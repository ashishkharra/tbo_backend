// config.js
const { Pool } = require('pg');

const key = 'hgffgf@#w2324233@#<&>rgrt5433@';
const NODE_ENV = 'development'

const pool = new Pool({
  host: 'localhost',
  user: 'tbo_election',
  password: 'Tbo@123',
  database: 'tbo_election',
  port: 5432,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

module.exports = { pool, key, NODE_ENV };
