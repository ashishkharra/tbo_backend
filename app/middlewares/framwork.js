const crypto = require('crypto');

module.exports = function(req, res, next) {
        const algorithm = 'aes-256-cbc';
        const key = crypto.randomBytes(32);
        const iv = crypto.randomBytes(16);

        const cipher = crypto.createCipheriv(algorithm, key, iv);

        let encrypted = cipher.update('Hello World', 'utf8', 'hex');
      encrypted += cipher.final('hex');

        const decipher = crypto.createDecipheriv(algorithm, key, iv);

        let decrypted = decipher.update(encrypted, 'hex', 'utf8');
        decrypted += decipher.final('utf8');

        next()
}