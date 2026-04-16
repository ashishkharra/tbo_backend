const express = require('express');
const cookieParser = require('cookie-parser');
const dotenv = require('dotenv');
const compression = require('compression');
const zlib = require('zlib');
const helmet = require('helmet');
dotenv.config();
const morgan = require('morgan');
const router = require('./app/routes/router');
const cors = require('cors');
const app = express();

const { connectRedis } = require('./app/config/redis.config.js');
const { initGlobalCache } = require('./app/controllers/dataId.controller.js');

const port = process.env.PORT || 3000;
const allowedOrigins = [
  'http://localhost:3000',
  'http://192.168.1.9:3000',
  'http://192.168.1.10:3000',
  'http://192.168.29.5:3000',
  'http://192.168.29.46:3000',
  'http://192.168.29.176:3000',
  'http://192.168.29.93:3000',
  'http://192.168.1.9:3000',
  'http://192.168.1.12:3000'
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error('Not allowed by CORS'));
  },
  credentials: true
}));

app.use('/', express.static('public'));
app.use(express.json());
app.use(cookieParser());
app.use(helmet());
app.use(
  compression({
    level: 6,
    threshold: 1024,
    filter: (req, res) => {
      if (req.headers['x-no-compression']) return false;
      return compression.filter(req, res);
    }
  })
);
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  console.log(`[LOG] ${req.method} ${req.url}`);
  next();
});

app.use((req, res, next) => {
  res.setHeader('Cache-Control', 'public, max-age=60');
  next();
});

app.get('/', (req, res) => {
  res.status(200).json({
    message: "Cloudflare is connected to Node.js!",
    time: new Date()
  });
});

app.use('/api', router);

app.listen(port, async () => {
  console.log(`Server is running on http://localhost:${port}`);
  
  try {
    await connectRedis();
    
    await initGlobalCache();
  } catch (err) {
    console.error("Initialization error:", err);
  }
});