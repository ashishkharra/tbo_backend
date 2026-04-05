const { createClient } = require('redis');
const { REDIS_URL } = require('./global.js')

const redisClient = createClient({
    url: REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('Redis Client Error', err));

const connectRedis = async () => {
    if (!redisClient.isOpen) {
        await redisClient.connect();
        console.log('Connected to Redis successfully');
    }
};

module.exports = { redisClient, connectRedis };