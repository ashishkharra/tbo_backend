const { NODE_ENV } = require('../config/global.js')
const AUTH_COOKIE_NAME = "access_token";

const getCookieOptions = () => {
  return {
    httpOnly: true,
    secure: NODE_ENV === 'production',
    sameSite: NODE_ENV === 'production' ? "none" : "lax",
    maxAge: 30 * 24 * 60 * 60 * 1000,
    path: "/",
  };
};

const setAuthCookie = (res, token) => {
  res.cookie(AUTH_COOKIE_NAME, token, getCookieOptions());
};

const clearAuthCookie = (res) => {
  res.clearCookie(AUTH_COOKIE_NAME, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: process.env.NODE_ENV === "production" ? "none" : "lax",
    path: "/",
  });
};

module.exports = {
  AUTH_COOKIE_NAME,
  getCookieOptions,
  setAuthCookie,
  clearAuthCookie,
};