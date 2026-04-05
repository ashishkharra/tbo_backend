// config.js
const { Pool } = require('pg');

const pool = new Pool({
  host: 'localhost',
  user: 'postgres',          
  password: 'admin', 
  database: 'tbo_data',
  port: 5432,                
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

module.exports = { pool };