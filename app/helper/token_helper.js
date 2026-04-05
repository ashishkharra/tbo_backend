const crypto = require("crypto");

const secretHex =
  "45c83d48463fb7e6dc5f3e4927725c5db6c8fae1210cd09fae1c3fc2356d3ecb";

if (!secretHex) {
  throw new Error("TOKEN_SECRET_KEY is required in environment");
}

const buff_key = Buffer.from(secretHex, "hex");

if (buff_key.length !== 32) {
  throw new Error("TOKEN_SECRET_KEY must be 32-byte hex string for aes-256-gcm");
}

const generateRawToken = () => {
  return crypto.randomBytes(48).toString("hex");
};

const hashToken = (token) => {
  return crypto.createHash("sha256").update(String(token)).digest("hex");
};

const encryptRefreshToken = (refreshToken) => {
  const iv = crypto.randomBytes(12);

  const cipher = crypto.createCipheriv("aes-256-gcm", buff_key, iv);

  const encrypted = Buffer.concat([
    cipher.update(String(refreshToken), "utf8"),
    cipher.final(),
  ]);

  return {
    iv: iv.toString("hex"),
    content: encrypted.toString("hex"),
    tag: cipher.getAuthTag().toString("hex"),
  };
};

const decryptRefreshToken = (encryptedData) => {
  if (
    !encryptedData ||
    typeof encryptedData !== "object" ||
    !encryptedData.iv ||
    !encryptedData.content ||
    !encryptedData.tag
  ) {
    throw new Error("Encrypted token payload is invalid");
  }

  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    buff_key,
    Buffer.from(encryptedData.iv, "hex")
  );

  decipher.setAuthTag(Buffer.from(encryptedData.tag, "hex"));

  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(encryptedData.content, "hex")),
    decipher.final(),
  ]);

  return decrypted.toString("utf8");
};

const encodeEncryptedToken = (encryptedObj) => {
  return Buffer.from(JSON.stringify(encryptedObj), "utf8").toString("base64url");
};

const decodeEncryptedToken = (encodedToken) => {
  try {
    if (!encodedToken || typeof encodedToken !== "string") {
      throw new Error("Encoded token is required and must be a string");
    }

    const decoded = Buffer.from(encodedToken.trim(), "base64url").toString("utf8");

    if (!decoded || !decoded.startsWith("{")) {
      throw new Error("Decoded token is not valid JSON");
    }

    const parsed = JSON.parse(decoded);

    if (
      !parsed ||
      typeof parsed !== "object" ||
      !parsed.iv ||
      !parsed.content ||
      !parsed.tag
    ) {
      throw new Error("Decoded token structure is invalid");
    }

    return parsed;
  } catch (error) {
    throw new Error(`Invalid encrypted token format: ${error.message}`);
  }
};

/**
 * New login/session ke liye
 */
const createEncryptedTokenForClient = () => {
  const rawToken = generateRawToken();
  const hashedToken = hashToken(rawToken);
  const encryptedTokenObj = encryptRefreshToken(rawToken);
  const encryptedToken = encodeEncryptedToken(encryptedTokenObj);

  return {
    rawToken,
    hashedToken,
    encryptedTokenObj,
    encryptedToken,
  };
};

/**
 * Existing raw token ko dubara encrypt karke client ko dene ke liye
 */
const encryptExistingRawTokenForClient = (rawToken) => {
  if (!rawToken || typeof rawToken !== "string") {
    throw new Error("rawToken is required");
  }

  const hashedToken = hashToken(rawToken);
  const encryptedTokenObj = encryptRefreshToken(rawToken);
  const encryptedToken = encodeEncryptedToken(encryptedTokenObj);

  return {
    rawToken,
    hashedToken,
    encryptedTokenObj,
    encryptedToken,
  };
};

module.exports = {
  generateRawToken,
  hashToken,
  encryptRefreshToken,
  decryptRefreshToken,
  encodeEncryptedToken,
  decodeEncryptedToken,
  createEncryptedTokenForClient,
  encryptExistingRawTokenForClient,
};