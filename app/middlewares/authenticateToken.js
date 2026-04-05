const jwt = require('jsonwebtoken');
const { pool, key, NODE_ENV } = require('../config/config');
const { decrypt } = require('../middlewares/customFunction');
const { decodeEncryptedToken, decryptRefreshToken, hashToken, createEncryptedTokenForClient } = require('../helper/token_helper');
const { AUTH_COOKIE_NAME, clearAuthCookie } = require('../helper/cookie_helper.js')

module.exports = {

  authenticateToken: async (req, res, next) => {
    const client = await pool.connect();

    try {
      const authHeader = req.headers.authorization;

      let incomingEncryptedToken = null;

      if (authHeader && authHeader.startsWith("Bearer ")) {
        incomingEncryptedToken = authHeader.replace("Bearer ", "").trim();
      } else if (req.cookies?.[AUTH_COOKIE_NAME]) {
        incomingEncryptedToken = String(req.cookies[AUTH_COOKIE_NAME]).trim();
      }

      if (!incomingEncryptedToken) {
        // clearAuthCookie(res);
        return res.status(401).json({
          success: false,
          sessionExpired: true,
          message: "Unauthorized access",
          data: {},
        });
      }

      if (typeof incomingEncryptedToken !== "string") {
        // clearAuthCookie(res);
        return res.status(401).json({
          success: false,
          sessionExpired: true,
          message: "Invalid access",
          data: {},
        });
      }

      let rawToken;
      let hashedToken;

      try {
        const decodedEncryptedToken = decodeEncryptedToken(incomingEncryptedToken);
        rawToken = decryptRefreshToken(decodedEncryptedToken);
        hashedToken = hashToken(rawToken);
      } catch (error) {
        console.log("Token decrypt failed:", error.message);

        // clearAuthCookie(res);

        return res.status(401).json({
          success: false,
          sessionExpired: true,
          message: "Invalid authentication token",
          data: {},
        });
      }

      await client.query("BEGIN");

      const sessionResult = await client.query(
        `
      SELECT
        us.id,
        us.user_id,
        us.user_token,
        us.session_token,
        us.platform,
        us.device_type,
        us.device_id,
        us.device_name,
        us.browser,
        us.os,
        us.ip_address,
        us.user_agent,
        us.personal_email,
        us.authenticated_email,
        us.login_type,
        us.login_access_used,
        us.is_active,
        us.is_logged_out,
        us.logout_reason,
        us.last_activity_at,
        us.logged_in_at,
        us.expires_at,
        u.id AS user_id_ref,
        u.username,
        u.email,
        u.authenticated_email AS user_authenticated_email,
        u.role,
        u.role_id,
        u.permission_set_id,
        u.permissions,
        u.module_permissions,
        u.permission_code,
        u.is_active AS user_is_active
      FROM user_sessions us
      INNER JOIN users u ON u.id = us.user_id
      WHERE us.session_token = $1
        AND us.is_active = true
        AND us.is_logged_out = false
        AND us.expires_at > NOW()
      LIMIT 1
      FOR UPDATE
      `,
        [hashedToken]
      );

      const session = sessionResult.rows[0];

      if (!session) {
        await client.query("ROLLBACK");
        // clearAuthCookie(res);

        return res.status(401).json({
          success: false,
          sessionExpired: true,
          message: "Session expired or invalid",
          data: {},
        });
      }

      if (!session.user_is_active) {
        await client.query("ROLLBACK");
        // clearAuthCookie(res);

        return res.status(403).json({
          success: false,
          sessionExpired: true,
          message: "User account is inactive",
          data: {},
        });
      }

      await client.query(
        `
      UPDATE user_sessions
      SET
        last_activity_at = NOW(),
        ip_address = $1,
        user_agent = $2,
        updated_at = NOW()
      WHERE id = $3
      `,
        [
          req.ip || req.headers["x-forwarded-for"] || session.ip_address || null,
          req.headers["user-agent"] || session.user_agent || null,
          session.id,
        ]
      );

      await client.query("COMMIT");

      req.user = {
        id: session.user_id,
        username: session.username,
        email: session.email,
        authenticated_email: session.user_authenticated_email,
        role: session.role,
        role_id: session.role_id,
        permission_set_id: session.permission_set_id,
        permissions: session.permissions,
        module_permissions: session.module_permissions,
        permission_code: session.permission_code,
      };

      req.session = {
        id: session.id,
        user_id: session.user_id,
        platform: session.platform,
        device_id: session.device_id,
        login_access_used: session.login_access_used,
      };

      next();
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.log("Rollback error:", rollbackError.message);
      }

      console.log("authenticateToken error:", error);

      // clearAuthCookie(res);

      return res.status(401).json({
        success: false,
        sessionExpired: true,
        message: "Authentication failed",
        data: {},
      });
    } finally {
      client.release();
    }
  },

  requireModulePermission: (moduleCode, actionCode) => {
    return async (req, res, next) => {
      try {
        if (!req.user) {
          return res.status(401).json({
            success: false,
            message: 'Authentication required'
          });
        }

        if (
          req.user.role === 'admin' ||
          req.user.role === 'super_admin' ||
          req.user.role === 'Admin' ||
          req.user.role === 'Super Admin'
        ) {
          return next();
        }

        const permissionResult = await pool.query(
          `
        SELECT up.id
        FROM user_permissions up
        JOIN permission_modules pm
          ON pm.id = up.module_id
        JOIN permission_actions pa
          ON pa.id = up.action_id
        WHERE up.user_id = $1
          AND pm.code = $2
          AND pa.code = $3
          AND up.is_allowed = TRUE
        LIMIT 1
        `,
          [req.user.id, moduleCode, actionCode]
        );

        if (permissionResult.rowCount === 0) {
          return res.status(403).json({
            success: false,
            message: `Missing permission: ${moduleCode}:${actionCode}`
          });
        }

        next();
      } catch (error) {
        console.error('Permission check error:', error);
        return res.status(500).json({
          success: false,
          message: 'Permission validation failed'
        });
      }
    };
  },

  requirePermission: (permission) => {
    return (req, res, next) => {
      if (!req.user) {
        return res.status(401).json({
          success: false,
          message: 'Authentication required'
        });
      }
      if (typeof req.user.hasPermission !== 'function') {
        return res.status(500).json({
          success: false,
          message: 'Permission system not initialized'
        });
      }
      if (!req.user.hasPermission(permission)) {
        return res.status(403).json({
          success: false,
          message: `Missing permission:${permission}`
        });
      }
      next();
    };
  },

  requireRole: (roles) => {
    return (req, res, next) => {
      if (!req.user) {
        return res.status(401).json({
          success: false,
          message: 'Authentication required'
        });
      }

      const allowedRoles = Array.isArray(roles) ? roles : [roles];

      if (!allowedRoles.includes(req.user.role)) {
        return res.status(403).json({
          success: false,
          message: 'Insufficient permissions'
        });
      }

      next();
    };
  }
};