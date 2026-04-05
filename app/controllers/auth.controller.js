const { pool } = require('../config/config.js')
const bcrypt = require("bcrypt");
const crypto = require('crypto')
const { key, NODE_ENV } = require("../config/global.js");
const customFunction = require("../middlewares/customFunction");
const authModel = require('../models/auth.model.js')
const { generateOtp, sendOtpToAuthenticatedEmail, maskEmail, hasLoginAccess, createSessionAndLogin, logEvent, DEVICE_OTP_EXPIRY_MINUTES, USER_OTP_EXPIRY_MINUTES, DEVICE_TRUST_DAYS, SESSION_EXPIRY_DAYS } = require('../middlewares/customFunction')
const { decodeEncryptedToken, decryptRefreshToken, hashToken } = require('../helper/token_helper.js')


const getGloballyTrustedDevice = async (client, { generated_device_id, platform }) => {
  const result = await client.query(
    `
    SELECT *
    FROM user_verified_devices
    WHERE generated_device_id = $1
      AND platform = $2
      AND is_verified = true
      AND is_revoked = false
      AND (expires_at IS NULL OR expires_at > NOW())
    ORDER BY id DESC
    LIMIT 1
    `,
    [generated_device_id, platform]
  );

  return result.rows[0] || null;
};

const markOldUnusedOtpsUsed = async ({
  client,
  user_id,
  device_id,
  platform,
  otp_type = null,
}) => {
  const params = [user_id, device_id, platform];
  let extra = "";

  if (otp_type) {
    params.push(otp_type);
    extra = ` AND otp_type = $4 `;
  }

  await client.query(
    `
    UPDATE user_login_otps
    SET
      is_used = true,
      used_at = NOW(),
      updated_at = NOW()
    WHERE user_id = $1
      AND device_id = $2
      AND platform = $3
      AND is_used = false
      ${extra}
    `,
    params
  );
};

const normalizeGeneratedDeviceId = (value) => {
  if (!value) return null;
  const v = String(value).trim();
  return v || null;
};

const getEffectiveGeneratedDeviceId = (body = {}) => {
  return normalizeGeneratedDeviceId(body.generated_device_id || body.device_id);
};

const buildFingerprintHash = ({
  platform,
  browser,
  os,
  user_agent,
  device_type,
  device_name,
}) => {
  const raw = [
    String(platform || "").trim().toUpperCase(),
    String(browser || "").trim().toUpperCase(),
    String(os || "").trim().toUpperCase(),
    String(user_agent || "").trim(),
    String(device_type || "").trim().toUpperCase(),
    String(device_name || "").trim().toUpperCase(),
  ].join("||");

  return crypto.createHash("sha256").update(raw).digest("hex");
};




const authController = {

  initiateLogin: async (req, res) => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const {
        mobile_no,
        password,
        platform,
        device_id,
        device_name,
        device_type,
        browser,
        os,
        ip_address,
        user_agent,
        login_access_requested,
      } = req.body;

      const generated_device_id = getEffectiveGeneratedDeviceId(req.body);

      if (!mobile_no || !password || !platform || !generated_device_id || !login_access_requested) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          message:
            "mobile_no, password, platform, generated_device_id and login_access_requested are required",
          data: {},
        });
      }

      const normalizedMobile = String(mobile_no).trim();
      const normalizedPlatform = String(platform).trim().toUpperCase();
      const normalizedAccess = String(login_access_requested).trim().toLowerCase();
      const normalizedGeneratedDeviceId = String(generated_device_id).trim();
      const normalizedDeviceId = String(device_id || generated_device_id).trim();

      const fingerprint_hash = buildFingerprintHash({
        platform: normalizedPlatform,
        browser,
        os,
        user_agent,
        device_type,
        device_name,
      });

      const userResult = await client.query(
        `
      SELECT
        id,
        token,
        username,
        email,
        mobile_no,
        password,
        authenticated_email,
        role,
        role_id,
        permission_set_id,
        is_active,
        email_verified,
        login_access
      FROM users
      WHERE mobile_no = $1
      LIMIT 1
      `,
        [normalizedMobile]
      );

      const user = userResult.rows[0];

      if (!user) {
        await client.query("ROLLBACK");
        return res.status(404).json({
          success: false,
          message: "User not found",
          data: {},
        });
      }

      if (!user.is_active) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Your account is inactive",
          data: {},
        });
      }

      if (!user.password) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Password is not configured for this user",
          data: {},
        });
      }

      let isPasswordValid = false;

      try {
        isPasswordValid = await bcrypt.compare(String(password), String(user.password));
      } catch (e) {
        isPasswordValid = String(password) === String(user.password);
      }

      if (!isPasswordValid) {
        await client.query("ROLLBACK");
        return res.status(401).json({
          success: false,
          message: "Invalid mobile number or password",
          data: {},
        });
      }

      if (!user.authenticated_email) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Authenticated email is not configured for this user",
          data: {},
        });
      }

      const accessAllowed = hasLoginAccess(user.login_access, normalizedAccess);

      if (!accessAllowed) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: `Login is not allowed for ${normalizedAccess}`,
          data: {
            allowed_access: user.login_access,
          },
        });
      }

      await client.query(
        `
      UPDATE user_sessions
      SET
        is_active = false,
        is_logged_out = true,
        logout_reason = 'NEW_LOGIN',
        logged_out_at = NOW(),
        updated_at = NOW()
      WHERE user_id = $1
        AND is_active = true
        AND is_logged_out = false
      `,
        [user.id]
      );

      const globallyTrustedDevice = await getGloballyTrustedDevice(client, {
        generated_device_id: normalizedGeneratedDeviceId,
        platform: normalizedPlatform,
      });

      console.log('gggggggg ', globallyTrustedDevice)

      await markOldUnusedOtpsUsed({
        client,
        user_id: user.id,
        device_id: normalizedGeneratedDeviceId,
        platform: normalizedPlatform,
      });

      if (globallyTrustedDevice) {
        await client.query(
          `
        INSERT INTO user_verified_devices (
          user_id,
          personal_email,
          authenticated_email,
          platform,
          login_access_type,
          device_type,
          device_id,
          generated_device_id,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          is_verified,
          verified_via,
          verified_at,
          expires_at,
          last_used_at,
          is_revoked,
          revoked_at,
          revoked_reason,
          verified_scope,
          verified_for_user_id,
          verification_status,
          created_at,
          updated_at,
          fingerprint_hash
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, true, 'GLOBAL_DEVICE_TRUST', NOW(), NOW() + INTERVAL '30 days', NOW(),
          false, NULL, NULL, 'GLOBAL_DEVICE', $1, 'VERIFIED', NOW(), NOW(), $14
        )
        ON CONFLICT (generated_device_id, platform)
        DO UPDATE SET
          user_id = EXCLUDED.user_id,
          personal_email = EXCLUDED.personal_email,
          authenticated_email = EXCLUDED.authenticated_email,
          login_access_type = EXCLUDED.login_access_type,
          device_type = EXCLUDED.device_type,
          device_id = EXCLUDED.device_id,
          device_name = EXCLUDED.device_name,
          browser = EXCLUDED.browser,
          os = EXCLUDED.os,
          ip_address = EXCLUDED.ip_address,
          user_agent = EXCLUDED.user_agent,
          is_verified = true,
          verified_via = 'GLOBAL_DEVICE_TRUST',
          verified_at = COALESCE(user_verified_devices.verified_at, NOW()),
          expires_at = NOW() + INTERVAL '30 days',
          last_used_at = NOW(),
          is_revoked = false,
          revoked_at = NULL,
          revoked_reason = NULL,
          verified_scope = 'GLOBAL_DEVICE',
          verified_for_user_id = EXCLUDED.verified_for_user_id,
          verification_status = 'VERIFIED',
          updated_at = NOW(),
          fingerprint_hash = EXCLUDED.fingerprint_hash
        `,
          [
            user.id,
            user.email || null,
            user.authenticated_email,
            normalizedPlatform,
            normalizedAccess,
            device_type || null,
            normalizedDeviceId,
            normalizedGeneratedDeviceId,
            device_name || null,
            browser || null,
            os || null,
            ip_address || null,
            user_agent || null,
            fingerprint_hash,
          ]
        );

        if (user.email_verified) {
          const sessionData = await createSessionAndLogin({
            client,
            user,
            normalizedPlatform,
            normalizedAccess,
            generated_device_id: normalizedGeneratedDeviceId,
            device_id: normalizedDeviceId,
            device_name,
            device_type,
            browser,
            os,
            ip_address,
            user_agent,
          });

          await logEvent(client, {
            user_id: user.id,
            event_type: "LOGIN_SUCCESS",
            platform: normalizedPlatform,
            device_id: normalizedGeneratedDeviceId,
            device_name,
            browser,
            os,
            ip_address,
            user_agent,
            personal_email: user.email || null,
            authenticated_email: user.authenticated_email,
            message: "New login successful on trusted generated device",
            meta_data: {
              mobile_no: user.mobile_no,
              trusted_device: true,
              trusted_device_source: "GENERATED_DEVICE_ID",
              user_verified: true,
              generated_device_id: normalizedGeneratedDeviceId,
            },
          });

          await client.query("COMMIT");

          return res.status(200).json({
            success: true,
            message: "Login successful on trusted device",
            data: {
              flow: "DIRECT_LOGIN",
              otp_required: false,
              trusted_device: true,
              device_already_verified: true,
              user_already_verified: true,
              token: sessionData.encryptedToken,
              session_token: sessionData.encryptedToken,
              expires_at: sessionData.expires_at,
              user: sessionData.user,
            },
          });
        }

        const userOtp = generateOtp();
        const userOtpExpiresAt = new Date(
          Date.now() + USER_OTP_EXPIRY_MINUTES * 60 * 1000
        );

        const userOtpInsertResult = await client.query(
          `
        INSERT INTO user_login_otps (
          user_id,
          personal_email,
          authenticated_email,
          otp_code,
          otp_type,
          platform,
          login_access_requested,
          device_type,
          device_id,
          generated_device_id,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          purpose,
          is_used,
          expires_at,
          created_at,
          updated_at
        )
        VALUES (
          $1, $2, $3, $4, 'USER_VERIFICATION_OTP', $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14, $15, false, $16, NOW(), NOW()
        )
        RETURNING id
        `,
          [
            user.id,
            user.email || null,
            user.authenticated_email,
            userOtp,
            normalizedPlatform,
            normalizedAccess,
            device_type || null,
            normalizedDeviceId,
            normalizedGeneratedDeviceId,
            device_name || null,
            browser || null,
            os || null,
            ip_address || null,
            user_agent || null,
            "User verification required. Device is already trusted",
            userOtpExpiresAt,
          ]
        );

        await sendOtpToAuthenticatedEmail({
          authenticatedEmail: user.authenticated_email,
          otp: userOtp,
          user,
        });

        await logEvent(client, {
          user_id: user.id,
          event_type: "OTP_SENT",
          platform: normalizedPlatform,
          device_id: normalizedGeneratedDeviceId,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          personal_email: user.email || null,
          authenticated_email: user.authenticated_email,
          message: "USER_VERIFICATION_OTP sent on trusted generated device",
          meta_data: {
            otp_request_id: userOtpInsertResult.rows[0].id,
            otp_type: "USER_VERIFICATION_OTP",
            mobile_no: user.mobile_no,
            trusted_device: true,
            user_verified: false,
            generated_device_id: normalizedGeneratedDeviceId,
          },
        });

        await client.query("COMMIT");

        return res.status(200).json({
          success: true,
          message: "User OTP sent successfully",
          data: {
            flow: "USER_OTP_SENT",
            otp_required: true,
            otp_type: "USER_VERIFICATION_OTP",
            otp_request_id: userOtpInsertResult.rows[0].id,
            masked_authenticated_email: maskEmail(user.authenticated_email),
            mobile_no: user.mobile_no,
            otp_expires_at: userOtpExpiresAt,
            trusted_device: true,
            device_already_verified: true,
            user_already_verified: false,
            next_step: "VERIFY_USER_OTP",
          },
        });
      }

      /**
       * FLOW B:
       * DEVICE NOT TRUSTED => send device OTP
       */
      const otpCode = generateOtp();
      const otpExpiresAt = new Date(
        Date.now() + DEVICE_OTP_EXPIRY_MINUTES * 60 * 1000
      );

      const otpInsertResult = await client.query(
        `
      INSERT INTO user_login_otps (
        user_id,
        personal_email,
        authenticated_email,
        otp_code,
        otp_type,
        platform,
        login_access_requested,
        device_type,
        device_id,
        generated_device_id,
        device_name,
        browser,
        os,
        ip_address,
        user_agent,
        purpose,
        is_used,
        expires_at,
        created_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, 'DEVICE_VERIFICATION_OTP', $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, false, $16, NOW(), NOW()
      )
      RETURNING id
      `,
        [
          user.id,
          user.email || null,
          user.authenticated_email,
          otpCode,
          normalizedPlatform,
          normalizedAccess,
          device_type || null,
          normalizedDeviceId,
          normalizedGeneratedDeviceId,
          device_name || null,
          browser || null,
          os || null,
          ip_address || null,
          user_agent || null,
          user.email_verified
            ? "Device verification required before login for verified user"
            : "Device verification required before user verification",
          otpExpiresAt,
        ]
      );

      await sendOtpToAuthenticatedEmail({
        authenticatedEmail: user.authenticated_email,
        otp: otpCode,
        user,
      });

      await logEvent(client, {
        user_id: user.id,
        event_type: "OTP_SENT",
        platform: normalizedPlatform,
        device_id: normalizedGeneratedDeviceId,
        device_name,
        browser,
        os,
        ip_address,
        user_agent,
        personal_email: user.email || null,
        authenticated_email: user.authenticated_email,
        message: "DEVICE_VERIFICATION_OTP sent",
        meta_data: {
          otp_request_id: otpInsertResult.rows[0].id,
          otp_type: "DEVICE_VERIFICATION_OTP",
          mobile_no: user.mobile_no,
          device_already_verified: false,
          user_already_verified: !!user.email_verified,
          generated_device_id: normalizedGeneratedDeviceId,
        },
      });

      await client.query("COMMIT");

      return res.status(200).json({
        success: true,
        message: "Device OTP sent successfully",
        data: {
          flow: "DEVICE_OTP_SENT",
          otp_required: true,
          otp_type: "DEVICE_VERIFICATION_OTP",
          otp_request_id: otpInsertResult.rows[0].id,
          masked_authenticated_email: maskEmail(user.authenticated_email),
          mobile_no: user.mobile_no,
          otp_expires_at: otpExpiresAt,
          device_already_verified: false,
          user_already_verified: !!user.email_verified,
          next_step: "VERIFY_DEVICE_OTP",
        },
      });
    } catch (error) {
      await client.query("ROLLBACK");
      console.log("initiateLogin error:", error);
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error",
        data: {},
      });
    } finally {
      client.release();
    }
  },

  verifyDeviceOtp: async (req, res) => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const {
        mobile_no,
        otp_code,
        otp_request_id,
        platform,
        device_id,
        device_name,
        device_type,
        browser,
        os,
        ip_address,
        user_agent,
        login_access_requested,
      } = req.body;

      const generated_device_id = getEffectiveGeneratedDeviceId(req.body);

      if (
        !mobile_no ||
        !otp_code ||
        !otp_request_id ||
        !platform ||
        !generated_device_id ||
        !login_access_requested
      ) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          message:
            "mobile_no, otp_code, otp_request_id, platform, generated_device_id and login_access_requested are required",
          data: {},
        });
      }

      const normalizedMobile = String(mobile_no).trim();
      const normalizedPlatform = String(platform).trim().toUpperCase();
      const normalizedAccess = String(login_access_requested).trim().toLowerCase();
      const normalizedGeneratedDeviceId = String(generated_device_id).trim();
      const normalizedDeviceId = String(device_id || generated_device_id).trim();

      const fingerprint_hash = buildFingerprintHash({
        platform: normalizedPlatform,
        browser,
        os,
        user_agent,
        device_type,
        device_name,
      });

      const userResult = await client.query(
        `
        SELECT
          id,
          token,
          username,
          email,
          mobile_no,
          authenticated_email,
          role,
          role_id,
          permission_set_id,
          is_active,
          email_verified,
          login_access
        FROM users
        WHERE mobile_no = $1
        LIMIT 1
        `,
        [normalizedMobile]
      );

      const user = userResult.rows[0];

      if (!user) {
        await client.query("ROLLBACK");
        return res.status(404).json({
          success: false,
          message: "User not found",
          data: {},
        });
      }

      if (!user.is_active) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Your account is inactive",
          data: {},
        });
      }

      if (!user.authenticated_email) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Authenticated email is not configured for this user",
          data: {},
        });
      }

      const accessAllowed = hasLoginAccess(user.login_access, normalizedAccess);

      if (!accessAllowed) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: `Login is not allowed for ${normalizedAccess}`,
          data: {},
        });
      }

      const otpResult = await client.query(
        `
        SELECT *
        FROM user_login_otps
        WHERE id = $1
          AND user_id = $2
          AND otp_code = $3
          AND otp_type = 'DEVICE_VERIFICATION_OTP'
          AND platform = $4
          AND generated_device_id = $5
          AND is_used = false
          AND expires_at > NOW()
        LIMIT 1
        `,
        [
          Number(otp_request_id),
          user.id,
          String(otp_code).trim(),
          normalizedPlatform,
          normalizedGeneratedDeviceId,
        ]
      );

      const otpRow = otpResult.rows[0];

      if (!otpRow) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          message: "Invalid or expired device OTP",
          data: {},
        });
      }

      await client.query(
        `
        UPDATE user_login_otps
        SET
          is_used = true,
          used_at = NOW(),
          updated_at = NOW()
        WHERE id = $1
        `,
        [otpRow.id]
      );

      const verifiedUntil = new Date(
        Date.now() + DEVICE_TRUST_DAYS * 24 * 60 * 60 * 1000
      );

      await client.query(
        `
        INSERT INTO user_verified_devices (
          user_id,
          personal_email,
          authenticated_email,
          platform,
          login_access_type,
          device_type,
          device_id,
          generated_device_id,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          is_verified,
          verified_via,
          verified_at,
          expires_at,
          last_used_at,
          is_revoked,
          revoked_at,
          revoked_reason,
          verified_scope,
          verified_for_user_id,
          verification_status,
          created_at,
          updated_at,
          fingerprint_hash
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, true, 'DEVICE_OTP', NOW(), $14, NOW(),
          false, NULL, NULL, 'GLOBAL_DEVICE', $1, 'VERIFIED', NOW(), NOW(), $15
        )
        ON CONFLICT (generated_device_id, platform)
        DO UPDATE SET
          user_id = EXCLUDED.user_id,
          personal_email = EXCLUDED.personal_email,
          authenticated_email = EXCLUDED.authenticated_email,
          login_access_type = EXCLUDED.login_access_type,
          device_type = EXCLUDED.device_type,
          device_id = EXCLUDED.device_id,
          device_name = EXCLUDED.device_name,
          browser = EXCLUDED.browser,
          os = EXCLUDED.os,
          ip_address = EXCLUDED.ip_address,
          user_agent = EXCLUDED.user_agent,
          is_verified = true,
          verified_via = 'DEVICE_OTP',
          verified_at = NOW(),
          expires_at = EXCLUDED.expires_at,
          last_used_at = NOW(),
          is_revoked = false,
          revoked_at = NULL,
          revoked_reason = NULL,
          verified_scope = 'GLOBAL_DEVICE',
          verified_for_user_id = EXCLUDED.verified_for_user_id,
          verification_status = 'VERIFIED',
          updated_at = NOW(),
          fingerprint_hash = EXCLUDED.fingerprint_hash
        `,
        [
          user.id,
          user.email || null,
          user.authenticated_email,
          normalizedPlatform,
          normalizedAccess,
          device_type || null,
          normalizedDeviceId,
          normalizedGeneratedDeviceId,
          device_name || null,
          browser || null,
          os || null,
          ip_address || null,
          user_agent || null,
          verifiedUntil,
          fingerprint_hash,
        ]
      );

      if (user.email_verified) {
        const sessionData = await createSessionAndLogin({
          client,
          user,
          normalizedPlatform,
          normalizedAccess,
          generated_device_id: normalizedGeneratedDeviceId,
          device_id: normalizedDeviceId,
          device_name,
          device_type,
          browser,
          os,
          ip_address,
          user_agent,
        });

        await logEvent(client, {
          user_id: user.id,
          event_type: "DEVICE_VERIFIED",
          platform: normalizedPlatform,
          device_id: normalizedGeneratedDeviceId,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          personal_email: user.email || null,
          authenticated_email: user.authenticated_email,
          message: "Device verified and login successful",
          meta_data: {
            mobile_no: user.mobile_no,
            trusted_device: true,
            user_verified: true,
            generated_device_id: normalizedGeneratedDeviceId,
          },
        });

        await client.query("COMMIT");

        return res.status(200).json({
          success: true,
          message: "Device verified successfully and login successful",
          data: {
            flow: "DIRECT_LOGIN",
            otp_required: false,
            trusted_device: true,
            device_already_verified: true,
            user_already_verified: true,
            token: sessionData.encryptedToken,
            session_token: sessionData.encryptedToken,
            expires_at: sessionData.expires_at,
            user: sessionData.user,
          },
        });
      }

      const userOtp = generateOtp();
      const userOtpExpiresAt = new Date(
        Date.now() + USER_OTP_EXPIRY_MINUTES * 60 * 1000
      );

      const userOtpInsertResult = await client.query(
        `
        INSERT INTO user_login_otps (
          user_id,
          personal_email,
          authenticated_email,
          otp_code,
          otp_type,
          platform,
          login_access_requested,
          device_type,
          device_id,
          generated_device_id,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          purpose,
          is_used,
          expires_at,
          created_at,
          updated_at
        )
        VALUES (
          $1, $2, $3, $4, 'USER_VERIFICATION_OTP', $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14, $15, false, $16, NOW(), NOW()
        )
        RETURNING id
        `,
        [
          user.id,
          user.email || null,
          user.authenticated_email,
          userOtp,
          normalizedPlatform,
          normalizedAccess,
          device_type || null,
          normalizedDeviceId,
          normalizedGeneratedDeviceId,
          device_name || null,
          browser || null,
          os || null,
          ip_address || null,
          user_agent || null,
          "User verification after successful device verification",
          userOtpExpiresAt,
        ]
      );

      await sendOtpToAuthenticatedEmail({
        authenticatedEmail: user.authenticated_email,
        otp: userOtp,
        user,
      });

      await logEvent(client, {
        user_id: user.id,
        event_type: "OTP_SENT",
        platform: normalizedPlatform,
        device_id: normalizedGeneratedDeviceId,
        device_name,
        browser,
        os,
        ip_address,
        user_agent,
        personal_email: user.email || null,
        authenticated_email: user.authenticated_email,
        message: "USER_VERIFICATION_OTP sent after device verification",
        meta_data: {
          otp_request_id: userOtpInsertResult.rows[0].id,
          otp_type: "USER_VERIFICATION_OTP",
          mobile_no: user.mobile_no,
          user_verified: false,
          generated_device_id: normalizedGeneratedDeviceId,
        },
      });

      await client.query("COMMIT");

      return res.status(200).json({
        success: true,
        message: "Device verified successfully. User OTP sent.",
        data: {
          flow: "USER_OTP_SENT",
          next_step: "VERIFY_USER_OTP",
          otp_type: "USER_VERIFICATION_OTP",
          otp_request_id: userOtpInsertResult.rows[0].id,
          masked_authenticated_email: maskEmail(user.authenticated_email),
          mobile_no: user.mobile_no,
          otp_expires_at: userOtpExpiresAt,
          device_already_verified: true,
          user_already_verified: false,
        },
      });
    } catch (error) {
      await client.query("ROLLBACK");
      console.log("verifyDeviceOtp error:", error);
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error",
        data: {},
      });
    } finally {
      client.release();
    }
  },

  verifyUserOtp: async (req, res) => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const {
        mobile_no,
        otp_code,
        otp_request_id,
        platform,
        device_id,
        device_name,
        device_type,
        browser,
        os,
        ip_address,
        user_agent,
        login_access_requested,
      } = req.body;

      const generated_device_id = getEffectiveGeneratedDeviceId(req.body);

      if (
        !mobile_no ||
        !otp_code ||
        !otp_request_id ||
        !platform ||
        !generated_device_id ||
        !login_access_requested
      ) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          message:
            "mobile_no, otp_code, otp_request_id, platform, generated_device_id and login_access_requested are required",
          data: {},
        });
      }

      const normalizedMobile = String(mobile_no).trim();
      const normalizedPlatform = String(platform).trim().toUpperCase();
      const normalizedAccess = String(login_access_requested).trim().toLowerCase();
      const normalizedGeneratedDeviceId = String(generated_device_id).trim();
      const normalizedDeviceId = String(device_id || generated_device_id).trim();

      const userResult = await client.query(
        `
        SELECT
          id,
          token,
          username,
          email,
          mobile_no,
          authenticated_email,
          role,
          role_id,
          permission_set_id,
          is_active,
          email_verified,
          login_access
        FROM users
        WHERE mobile_no = $1
        LIMIT 1
        `,
        [normalizedMobile]
      );

      const user = userResult.rows[0];

      if (!user) {
        await client.query("ROLLBACK");
        return res.status(404).json({
          success: false,
          message: "User not found",
          data: {},
        });
      }

      if (!user.is_active) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Your account is inactive",
          data: {},
        });
      }

      if (!user.authenticated_email) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: "Authenticated email is not configured for this user",
          data: {},
        });
      }

      const accessAllowed = hasLoginAccess(user.login_access, normalizedAccess);

      if (!accessAllowed) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          message: `Login is not allowed for ${normalizedAccess}`,
          data: {},
        });
      }

      const globalDeviceCheck = await getGloballyTrustedDevice(client, {
        generated_device_id: normalizedGeneratedDeviceId,
        platform: normalizedPlatform,
      });

      if (!globalDeviceCheck) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          message: "Device is not verified. Please verify device first.",
          data: {},
        });
      }

      const otpResult = await client.query(
        `
        SELECT *
        FROM user_login_otps
        WHERE id = $1
          AND user_id = $2
          AND otp_code = $3
          AND otp_type = 'USER_VERIFICATION_OTP'
          AND platform = $4
          AND generated_device_id = $5
          AND is_used = false
          AND expires_at > NOW()
        LIMIT 1
        `,
        [
          Number(otp_request_id),
          user.id,
          String(otp_code).trim(),
          normalizedPlatform,
          normalizedGeneratedDeviceId,
        ]
      );

      const otpRow = otpResult.rows[0];

      if (!otpRow) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          message: "Invalid or expired user OTP",
          data: {},
        });
      }

      await client.query(
        `
        UPDATE user_login_otps
        SET
          is_used = true,
          used_at = NOW(),
          updated_at = NOW()
        WHERE id = $1
        `,
        [otpRow.id]
      );

      await client.query(
        `
        UPDATE users
        SET
          email_verified = true,
          updated_at = NOW()
        WHERE id = $1
        `,
        [user.id]
      );

      await client.query(
        `
        UPDATE user_verified_devices
        SET
          user_id = $1,
          personal_email = $2,
          authenticated_email = $3,
          device_type = COALESCE($4, device_type),
          device_id = COALESCE($5, device_id),
          device_name = COALESCE($6, device_name),
          browser = COALESCE($7, browser),
          os = COALESCE($8, os),
          ip_address = COALESCE($9, ip_address),
          user_agent = COALESCE($10, user_agent),
          login_access_type = $11,
          is_verified = true,
          is_revoked = false,
          verified_scope = 'GLOBAL_DEVICE',
          verified_for_user_id = $1,
          verification_status = 'VERIFIED',
          expires_at = NOW() + INTERVAL '30 days',
          last_used_at = NOW(),
          updated_at = NOW()
        WHERE generated_device_id = $12
          AND platform = $13
        `,
        [
          user.id,
          user.email || null,
          user.authenticated_email,
          device_type || null,
          normalizedDeviceId,
          device_name || null,
          browser || null,
          os || null,
          ip_address || null,
          user_agent || null,
          normalizedAccess,
          normalizedGeneratedDeviceId,
          normalizedPlatform,
        ]
      );

      const sessionData = await createSessionAndLogin({
        client,
        user: {
          ...user,
          email_verified: true,
        },
        normalizedPlatform,
        normalizedAccess,
        generated_device_id: normalizedGeneratedDeviceId,
        device_id: normalizedDeviceId,
        device_name,
        device_type,
        browser,
        os,
        ip_address,
        user_agent,
      });

      await logEvent(client, {
        user_id: user.id,
        event_type: "LOGIN_SUCCESS",
        platform: normalizedPlatform,
        device_id: normalizedGeneratedDeviceId,
        device_name,
        browser,
        os,
        ip_address,
        user_agent,
        personal_email: user.email || null,
        authenticated_email: user.authenticated_email,
        message: "User verification successful and login completed",
        meta_data: {
          mobile_no: user.mobile_no,
          trusted_device: true,
          user_verified: true,
          generated_device_id: normalizedGeneratedDeviceId,
        },
      });

      await client.query("COMMIT");

      return res.status(200).json({
        success: true,
        message: "Login successful",
        data: {
          flow: "DIRECT_LOGIN",
          otp_required: false,
          trusted_device: true,
          device_already_verified: true,
          user_already_verified: true,
          token: sessionData.encryptedToken,
          session_token: sessionData.encryptedToken,
          expires_at: sessionData.expires_at,
          user: sessionData.user,
        },
      });
    } catch (error) {
      await client.query("ROLLBACK");
      console.log("verifyUserOtp error:", error);
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error",
        data: {},
      });
    } finally {
      client.release();
    }
  },

  logout: async (req, res) => {
    const client = await pool.connect();

    try {
      const authHeader = req.headers.authorization;
      let incomingEncryptedToken = null;

      if (authHeader && authHeader.startsWith("Bearer ")) {
        incomingEncryptedToken = authHeader.replace("Bearer ", "").trim();
      }

      if (!incomingEncryptedToken) {
        return res.status(401).json({
          success: false,
          message: "Authentication token missing",
          data: {},
        });
      }

      const decodedEncryptedToken = decodeEncryptedToken(incomingEncryptedToken);
      const rawToken = decryptRefreshToken(decodedEncryptedToken);
      const hashedToken = hashToken(rawToken);

      await client.query("BEGIN");

      await client.query(
        `
      UPDATE user_sessions
      SET
        is_active = false,
        is_logged_out = true,
        logout_reason = 'USER_LOGOUT',
        updated_at = NOW()
      WHERE session_token = $1
      `,
        [hashedToken]
      );

      await client.query("COMMIT");

      return res.status(200).json({
        success: true,
        message: "Logged out successfully",
        data: {},
      });
    } catch (error) {
      await client.query("ROLLBACK");
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error",
        data: {},
      });
    } finally {
      client.release();
    }
  },

  postUser: async (req, res) => {
    try {
      const {
        username,
        email,
        authenticatedEmail,
        password,
        mobile,
        role,
        permissions
      } = req.body.data;

      console.log('reab ', req.body)

      if (!username || !email || !password || !mobile) {
        return res.status(400).json({
          success: false,
          message: 'Username, email, password, and mobile are required'
        });
      }
      let loginAccessArray = [];

      if (typeof permissions === 'string') {
        loginAccessArray = permissions.split(',').map(p => p.trim()).filter(p => p);
      } else if (Array.isArray(permissions)) {
        loginAccessArray = permissions;
      } else if (!permissions) {
        loginAccessArray = ['web'];
      }
      const normalizedLoginAccess = {
        web: loginAccessArray.includes('web'),
        mobile: loginAccessArray.includes('mobile')
      };

      if (!normalizedLoginAccess.web && !normalizedLoginAccess.mobile) {
        return res.status(400).json({
          success: false,
          message: 'At least one login access (web or mobile) must be enabled'
        });
      }
      const loginAccessForDB = {
        web: normalizedLoginAccess.web,
        mobile: normalizedLoginAccess.mobile
      };

      if (req.user.role === 'admin' && role === 'super_admin') {
        return res.status(403).json({
          success: false,
          message: 'Admins cannot create super admin users'
        });
      }

      if (req.user.role === 'leader' && role !== 'data_entry_operator') {
        return res.status(403).json({
          success: false,
          message: 'Leaders can only create data entry operator users'
        });
      }

      const existingUser = await authModel.findExistingUser({
        username,
        email,
        mobile
      });

      if (existingUser) {
        return res.status(400).json({
          success: false,
          message: 'Username, email, or mobile already exists'
        });
      }
      const encryptedPassword = customFunction.encrypt(password, key);
      const result = await authModel.createUser({
        username,
        email,
        authenticated_email: authenticatedEmail || null,
        password: encryptedPassword,
        mobile: mobile || null,
        email_verified: true,
        role,
        login_access: loginAccessForDB,
        created_by: req.user.id
      });

      return res.status(201).json({
        success: true,
        message: 'User created successfully',
        data: {
          id: result.id,
          username,
          email,
          mobile: mobile || null,
          role,
          permissions: normalizedLoginAccess
        }
      });

    } catch (error) {
      console.error('❌ User creation error:', error);
      return res.status(500).json({
        success: false,
        message: 'Server error: ' + error.message
      });
    }
  },

  getUsers: async (req, res) => {
    try {
      const filters = {
        role: req.query.role || "all",
        parent_id: req.query.parent_id || "all",
        username: req.query.username || "all",
        status: req.query.status || "all",
        search: req.query.search || "",
        page: Number(req.query.page || 1),
        limit: Number(req.query.limit || 1000),
      };

      const result = await authModel.fetchUsers(req.user, filters);
      const teams = await authModel.fetchTeams();

      const teamMap = {};
      for (const t of teams) {
        teamMap[t.id] = {
          team_id: t.id,
          team_name: t.name,
          team_code: t.team_code
        };
      }

      const parseJSON = (value, fallback) => {
        if (!value || value === "null") return fallback;
        try {
          return typeof value === "string" ? JSON.parse(value) : value;
        } catch {
          return fallback;
        }
      };

      const data = result.rows.map(u => {
        const teamIds = u.team_id
          ? String(u.team_id).split(",").map(Number).filter(Boolean)
          : [];

        return {
          ...u,
          permissions: parseJSON(u.permissions, []),
          assignedmodules: parseJSON(u.assignedmodules, []),
          assignedsubmodules: parseJSON(u.assignedsubmodules, []),
          assigneddatasets: parseJSON(u.assigneddatasets, []),
          datasetaccess: parseJSON(u.datasetaccess, {}),
          dataassignment: parseJSON(u.dataassignment, null),
          hierarchicaldataassignment: parseJSON(u.hierarchicaldataassignment, null),
          module_permissions: parseJSON(u.module_permissions, {}),
          assignment_tokens: u.assignment_tokens
            ? u.assignment_tokens.split(",")
            : [],
          assignment_token: u.assignment_tokens
            ? u.assignment_tokens.split(",")[0]
            : null,
          team_id: teamIds[0] || null,
          team_ids: teamIds,
          teams: teamIds.map(id => teamMap[id]).filter(Boolean)
        };
      });

      return res.status(200).json({
        success: true,
        data,
        pagination: {
          currentPage: filters.page,
          itemsPerPage: filters.limit,
          totalItems: result.total,
          totalPages: Math.ceil(result.total / filters.limit)
        }
      });

    } catch (error) {
      console.error("GET USERS ERROR:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to fetch users"
      });
    }
  },

  bulkUpdateUsers: async (req, res) => {
    try {
      const { users } = req.body;
      if (!Array.isArray(users) || !users.length) {
        return res.status(400).json({
          success: false,
          message: "users array is required",
        });
      }

      const result = await authModel.bulkUpdateUsers(users, req.user);

      return res.status(200).json({
        success: true,
        message: "Users updated successfully",
        data: result,
      });
    } catch (error) {
      console.error("BULK UPDATE USERS ERROR:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to update users",
      });
    }
  },

  getUserDetails: async (req, res) => {
    try {
      const userId = Number(req.params.id);

      if (!userId || Number.isNaN(userId)) {
        return res.status(400).json({
          success: false,
          message: "Valid user id is required",
          data: {}
        });
      }

      const result = await authModel.getUserDetails(userId);

      if (!result) {
        return res.status(404).json({
          success: false,
          message: "User not found",
          data: {}
        });
      }

      return res.status(200).json({
        success: true,
        message: "User details fetched successfully",
        data: result
      });
    } catch (error) {
      console.error("❌ getUserDetails error:", error);
      return res.status(500).json({
        success: false,
        message: "Server error: " + error.message,
        data: {}
      });
    }
  },

  getModulesCode: async (req, res) => {
    try {
      const rows = await authModel.getAllModulesCode();
      const data = (rows || []).map(r => ({
        payload: String(r.payload),
        code: Number(r.code)
      }));

      return res.json({ success: true, data });
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: "Failed to fetch modules_code",
        error: error.message
      });
    }
  },

  updateUser: async (req, res) => {
    try {
      const { id } = req.params;
      const payload = { ...req.body };

      if (payload.mobile !== undefined) {
        payload.mobile_no = payload.mobile;
        delete payload.mobile;
      }

      const user = await authModel.getUserById(id);
      if (!user) {
        return res.status(404).json({
          success: false,
          message: "User not found",
        });
      }

      if (
        req.user.role === "admin" &&
        (user.role === "super_admin" || payload.role === "super_admin")
      ) {
        return res.status(403).json({
          success: false,
          message: "Access denied",
        });
      }

      if (payload.assignedModules || payload.assignedDatasets) {
        payload.modules_code = await generateModulesCode(
          payload.assignedModules || [],
          payload.assignedDatasets || [],
          payload.assignedSubModules || [],
          payload.assignedNavbarPages || [],
          id
        );
      }

      const updatedUser = await authModel.updateUserById(id, payload);

      return res.json({
        success: true,
        message: "User updated successfully",
        data: updatedUser,
      });
    } catch (err) {
      console.error("Update User Error:", err);
      return res.status(500).json({
        success: false,
        message: "Update failed",
      });
    }
  },

  deleteUser: async (req, res) => {
    try {
      const { id } = req.params;

      const user = await authModel.getUserById(id);
      if (!user) {
        return res.status(404).json({
          success: false,
          message: "User not found",
        });
      }

      await deleteUserById(id);

      return res.json({
        success: true,
        message: "User permanently deleted",
      });
    } catch (error) {
      console.error("deleteUser controller error:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to delete user",
      });
    }
  },

  updateUserPassword: async (req, res) => {
    try {
      const { id } = req.params;
      const { newPassword } = req.body || {};

      if (!newPassword) {
        return res.status(400).json({
          success: false,
          message: "New password is required",
        });
      }

      const user = await authModel.getUserById(id);
      if (!user) {
        return res.status(404).json({
          success: false,
          message: "User not found",
        });
      }

      if (
        req.user.role === "admin" &&
        user.role === "super_admin"
      ) {
        return res.status(403).json({
          success: false,
          message: "Admins cannot modify super admin users",
        });
      }

      const encryptedPassword = customFunction.encrypt(password, key);
      await updateUserPasswordById(id, encryptedPassword);

      return res.json({
        success: true,
        message: "Password updated successfully",
      });
    } catch (error) {
      console.error("updateUserPassword error:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to update password",
      });
    }
  },

  changeOwnPassword: async (req, res) => {
    try {
      const { currentPassword, newPassword } = req.body || {};
      const userId = req.params.id;

      if (!currentPassword || !newPassword) {
        return res.status(400).json({
          success: false,
          message: "Current password and new password are required",
        });
      }

      const user = await authModel.getUserById(userId);
      if (!user) {
        return res.status(404).json({
          success: false,
          message: "User not found",
        });
      }

      const decrypted = customFunction.decrypt(user.password, key);
      if (currentPassword !== decrypted) {
        return res.status(401).json({
          success: false,
          message: "Invalid credentials"
        });
      }

      const encryptedPassword = customFunction.encrypt(newPassword, key);
      await authModel.updateUserPasswordById(userId, encryptedPassword);

      return res.json({
        success: true,
        message: "Password changed successfully",
      });
    } catch (error) {
      console.error("changeOwnPassword error:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to change password",
      });
    }
  },

  getProfile: async (req, res) => {
    try {
      const { token } = req.user;
      const result = await authModel.getProfileModel(token)

      if (!result.rowCount) {
        return res.status(404).json({
          success: false,
          message: "User not found"
        });
      }

      const u = result.rows[0];

      const permissions = u.permissions ? JSON.parse(u.permissions) : [];
      const assignedModules = u.assignedmodules ? JSON.parse(u.assignedmodules) : [];
      const assignedSubModules = u.assignedsubmodules ? JSON.parse(u.assignedsubmodules) : [];
      const assignedDatasets = u.assigneddatasets ? JSON.parse(u.assigneddatasets) : [];
      const datasetAccess = u.datasetaccess ? JSON.parse(u.datasetaccess) : {};
      const dataAssignment = u.dataassignment ? JSON.parse(u.dataassignment) : null;
      const hierarchicalDataAssignment = u.hierarchicaldataassignment
        ? JSON.parse(u.hierarchicaldataassignment)
        : null;
      const modulePermissions = u.module_permissions ? JSON.parse(u.module_permissions) : {};

      res.status(200).json({
        success: true,
        data: {
          user: {
            id: u.id,
            username: u.username,
            email: u.email,
            mobile_no: u.mobile_no,
            role: u.role,
            team_id: u.team_id,
            parent_id: u.parent_id,
            is_active: u.is_active,
            permissions,
            assignedModules,
            assignedSubModules,
            assignedDatasets,
            datasetAccess,
            dataAssignment,
            hierarchicalDataAssignment,
            modulePermissions,
            created_at: u.created_at,
            updated_at: u.updated_at
          },
          accessibleResources: {
            users: permissions.includes("users:read"),
            voters: permissions.includes("voters:read"),
            reports: permissions.includes("reports:read"),
            mobile: permissions.includes("mobile:access"),
            adminPanel: u.role === "admin" || u.role === "super_admin"
          }
        }
      });
    } catch (err) {
      res.status(500).json({
        success: false,
        message: "Failed to fetch profile"
      });
    }
  },

  getUserPermissions: async (req, res) => {
    try {
      const { userId } = req.params;
      const result = await authModel.getUserPermissions(userId);
      return res.status(200).json({
        success: true,
        data: result,
      });
    } catch (error) {
      console.log("Get user permissions error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  },

  assignModulesToUser: async (req, res) => {
    try {
      const result = await authModel.assignModulesToUser(req.body);
      return res.status(200).json({
        success: true,
        message: "Modules and permissions assigned successfully",
        data: result,
      });
    } catch (error) {
      console.log("Assign modules to user error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  },

  applyModulesCodeToUser: async (req, res) => {
    try {
      const result = await authModel.applyModulesCodeToUser(req.body);

      return res.status(200).json({
        success: true,
        message: "Modules copied successfully",
        data: result,
      });
    } catch (error) {
      console.log("Apply modules code to user error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  },

  getUserAssignedModules: async (req, res) => {
    try {
      const { userId } = req.params;
      const result = await authModel.getUserAssignedModules(userId);

      return res.status(200).json({
        success: true,
        data: result,
      });
    } catch (error) {
      console.log("Get user assigned modules error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  },




  // assignUserPermissions: async (req, res) => {
  //   try {
  //     const result = await authModel.assignUserPermissions(req.body);

  //     return res.status(200).json({
  //       success: true,
  //       message: "User permissions assigned successfully",
  //       data: result,
  //     });
  //   } catch (error) {
  //     console.log("Assign user permissions error:", error);
  //     return res.status(500).json({
  //       success: false,
  //       message: error.message,
  //     });
  //   }
  // },
  // applyPermissionSetToUser: async (req, res) => {
  //   try {
  //     const result = await authModel.applyPermissionSetToUser(req.body);

  //     return res.status(200).json({
  //       success: true,
  //       message: "Permission set applied successfully",
  //       data: result,
  //     });
  //   } catch (error) {
  //     console.log("Apply permission set error:", error);
  //     return res.status(500).json({
  //       success: false,
  //       message: error.message,
  //     });
  //   }
  // },
  // getUserPermissions: async (req, res) => {
  //   try {
  //     const { userId } = req.params;
  //     const result = await authModel.getUserPermissions(userId);
  //     return res.status(200).json({
  //       success: true,
  //       data: result,
  //     });
  //   } catch (error) {
  //     console.log("Get user permissions error:", error);
  //     return res.status(500).json({
  //       success: false,
  //       message: error.message,
  //     });
  //   }
  // },
  // getPermissionSetDetails: async (req, res) => {
  //   try {
  //     const { permissionSetId } = req.params;
  //     const result = await authModel.getPermissionSetDetails(permissionSetId);

  //     return res.status(200).json({
  //       success: true,
  //       data: result,
  //     });
  //   } catch (error) {
  //     console.log("Get permission set details error:", error);
  //     return res.status(500).json({
  //       success: false,
  //       message: error.message,
  //     });
  //   }
  // },
  // getPermissionSets: async (req, res) => {
  //   try {
  //     const result = await authModel.getPermissionSets();

  //     return res.status(200).json({
  //       success: true,
  //       data: result,
  //     });
  //   } catch (error) {
  //     console.log("Get permission sets error:", error);
  //     return res.status(500).json({
  //       success: false,
  //       message: error.message,
  //     });
  //   }
  // },


  getPermissionModules: async (req, res) => {
    try {
      const result = await authModel.getPermissionModules()
      return res.status(200).json({
        success: true,
        message: "Permission modules fetched successfully",
        data: result
      })
    } catch (error) {
      console.log('Getting permission modules error : ', error)
      return res.status(500).json({
        success: false,
        message: error.message,
        data: {}
      })
    }
  },

  getTableColumns: async (req, res) => {
    try {
      const { table } = req.query
      const result = await authModel.getTableColumns(table)
      return res.status(200).json({
        success: true,
        message: "Table columns fetched successfully",
        data: result
      })
    } catch (error) {
      console.log('Getting table columns  error : ', error)
      return res.status(500).json({
        success: false,
        message: error.message,
        data: {}
      })
    }
  },

  getAssignmentColumnPermissions: async (req, res) => {
    try {
      const { user_id, assignment_id, db_table } = req.query;
      const owner_id = req.user?.id;

      if (!user_id || !assignment_id || !db_table) {
        return res.status(400).json({
          success: false,
          message: 'user_id, assignment_id and db_table are required',
          data: {}
        });
      }

      const result = await authModel.getAssignmentColumnPermissions({
        user_id: Number(user_id),
        assignment_id: Number(assignment_id),
        db_table,
        owner_id: Number(owner_id)
      });

      return res.status(200).json({
        success: true,
        message: 'Assignment column permissions fetched successfully',
        data: result
      });
    } catch (error) {
      console.log('Getting assignment column permissions error : ', error);
      return res.status(500).json({
        success: false,
        message: error.message,
        data: {}
      });
    }
  },

  getAssignableData: async (req, res) => {
    try {
      const { table, wise } = req.body
      const result = await authModel.getAssignableData({ table, wise })
      return res.status(200).json({
        success: true,
        message: 'Data fetched successfully',
        data: result
      })
    } catch (error) {
      console.log('Getting assignable data error : ', error)
      return res.status(500).json({
        success: false,
        message: error.message
      })
    }
  }


}
module.exports = authController;
