const { execSync } = require('child_process');
const crypto = require('crypto');
const nodemailer = require("nodemailer");
const { createEncryptedTokenForClient } = require('../helper/token_helper');


exports.DEVICE_OTP_EXPIRY_MINUTES = 10
exports.USER_OTP_EXPIRY_MINUTES = 10;
exports.DEVICE_TRUST_DAYS = 30;
exports.SESSION_EXPIRY_DAYS = 30;

const transporter = nodemailer.createTransport({
  host: "smtp.hostinger.com",
  port: 465,
  secure: true,
  auth: {
    user: "hello@tbo.services",
    pass: 'Hello@0993',
  },
});

exports.GetSession = (session) => {
  return session
}

//exports.passwordEncrypt = (password) => {
//    const saltRounds = 10;
//    return bcrypt.hashSync(password, saltRounds);
//}
//
//exports.passwordDecrypt = (password, hash) => {
//    return bcrypt.compareSync(password, hash);
//}

exports.validateEmail = (email) => {
  const regex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return regex.test(email);
};


exports.capitalizeFirstLetter = (string) => {
  if (!string) return '';
  return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
}

exports.formatDate = (date, format) => {
  const moment = require('moment');
  return moment(date).format(format);
}

exports.CurrentDateFunction = () => {
  const currentDate = new Date();
  return currentDate;
}

exports.DateFunction = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  const day = now.getDate();
  const date = month + '-' + day + '-' + year
  return date;
}

exports.getNextDate = (dateString) => {
  // Split the input string into day, month, and year
  const [day, month, year] = dateString.split('-').map(num => parseInt(num, 10));

  // Create a new Date object (months are 0-based in JavaScript, so subtract 1 from the month)
  const date = new Date(year, month - 1, day);

  // Add one day
  date.setDate(date.getDate() + 1);

  // Format the new date back to dd-mm-yyyy
  const newDay = String(date.getDate()).padStart(2, '0');
  const newMonth = String(date.getMonth() + 1).padStart(2, '0');
  const newYear = date.getFullYear();

  return `${newDay}-${newMonth}-${newYear}`;
}

exports.DateAndTimeFunction = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  const day = now.getDate();
  const hours = now.getHours();
  const minutes = now.getMinutes();
  const seconds = now.getSeconds();
  const date = day + '-' + month + '-' + year + ' ' + hours + ':' + minutes + ':' + seconds
  return date;
}

exports.getDateAndTime = () => {
  const date = new Date();
  return date.toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric', hour12: true });
}

exports.getFormattedDate = (format) => {
  const date = new Date();
  const moment = require('moment');
  return moment(date).format(format);
}

exports.autoRefresh = (res, duration) => {
  res.setHeader("Refresh", duration);
}


exports.phoneValidation = (mobile) => {
  const regex = /^[0-9]{10}$/;
  return regex.test(mobile);
}

exports.pincodeValidation = (pincode) => {
  const regex = /^[0-9]{6}$/;
  return regex.test(pincode);
}

exports.compressImage = (source, destination, quality) => {
  return sharp(source)
    .jpeg({ quality })
    .toFile(destination);
}

exports.generateRandomString = (length) => {
  const characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
}

exports.generateRandomNumber = () => {
  return Math.floor(10000000 + Math.random() * 90000000);
}

exports.generateToken = (length) => {
  return crypto.randomBytes(length).toString('hex');
}

exports.generateRandomNumber = () => {
  return Math.floor(10000000 + Math.random() * 90000000);
}

exports.generateOtp = () => {
  return Math.floor(100000 + Math.random() * 900000);
}

exports.hasLoginAccess = (loginAccess, requestedAccess) => {
  if (!loginAccess || !requestedAccess) return false;
  let parsed = loginAccess;
  if (typeof loginAccess === "string") {
    try {
      parsed = JSON.parse(loginAccess);
    } catch (error) {
      return false;
    }
  }
  const normalizedAccess = String(requestedAccess).trim().toLowerCase();
  return parsed?.[normalizedAccess] === true;
};

exports.maskEmail = (email) => {
  if (!email || !email.includes("@")) return email || "";
  const [name, domain] = email.split("@");
  if (name.length <= 2) return `${name[0]}***@${domain}`;
  return `${name.slice(0, 2)}***@${domain}`;
};

// replace with your real mail function
exports.sendOtpToAuthenticatedEmail = async ({ authenticatedEmail, otp, user }) => {
  try {
    const displayName =
      user?.username?.trim() ||
      user?.email?.trim() ||
      "User";

    const currentYear = new Date().getFullYear();

    const mailOptions = {
      from: `"TBO Services" <hello@tbo.services>`,
      to: authenticatedEmail,
      subject: "Your OTP Code for Secure Verification",
      text: `
Hello ${displayName},

Your One-Time Password (OTP) is: ${otp}

This OTP is valid for 5 minutes.

If you did not request this, please ignore this email.

© ${currentYear} TBO Services. All rights reserved.
            `.trim(),
      html: `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OTP Verification</title>
</head>
<body style="margin:0; padding:0; background-color:#edf2f7; font-family:Arial, Helvetica, sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#edf2f7; margin:0; padding:30px 15px;">
    <tr>
      <td align="center">

        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:600px; background-color:#ffffff; border-radius:20px; overflow:hidden; box-shadow:0 12px 40px rgba(15, 23, 42, 0.12);">
          
          <!-- Top Banner -->
          <tr>
            <td style="background:linear-gradient(135deg, #0f172a 0%, #1d4ed8 55%, #2563eb 100%); padding:36px 30px; text-align:center;">
              <div style="display:inline-block; width:64px; height:64px; line-height:64px; border-radius:16px; background:rgba(255,255,255,0.14); color:#ffffff; font-size:30px; font-weight:bold; text-align:center;">
                🔐
              </div>
              <h1 style="margin:18px 0 8px; color:#ffffff; font-size:28px; line-height:36px; font-weight:700;">
                OTP Verification
              </h1>
              <p style="margin:0; color:rgba(255,255,255,0.88); font-size:15px; line-height:24px;">
                Secure access confirmation for your account
              </p>
            </td>
          </tr>

          <!-- Main Content -->
          <tr>
            <td style="padding:40px 34px 24px;">
              <p style="margin:0 0 14px; font-size:18px; line-height:28px; color:#111827; font-weight:600;">
                Hello ${displayName},
              </p>

              <p style="margin:0 0 24px; font-size:15px; line-height:26px; color:#4b5563;">
                We received a request to verify your identity. Use the one-time password below to continue securely.
              </p>

              <!-- OTP Card -->
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 24px; background:linear-gradient(180deg, #eff6ff 0%, #dbeafe 100%); border:1px solid #bfdbfe; border-radius:18px;">
                <tr>
                  <td align="center" style="padding:28px 20px 24px;">
                    <p style="margin:0 0 10px; font-size:13px; line-height:20px; color:#1d4ed8; font-weight:700; letter-spacing:1.2px; text-transform:uppercase;">
                      Your Verification Code
                    </p>
                    <div style="display:inline-block; padding:16px 24px; background:#ffffff; border:2px dashed #3b82f6; border-radius:14px; font-size:34px; line-height:40px; font-weight:800; letter-spacing:10px; color:#0f172a; box-shadow:0 8px 20px rgba(59, 130, 246, 0.10);">
                      ${otp}
                    </div>
                    <p style="margin:14px 0 0; font-size:13px; line-height:22px; color:#475569;">
                      This OTP will expire in <strong>5 minutes</strong>.
                    </p>
                  </td>
                </tr>
              </table>

              <!-- Info Box -->
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 22px; background-color:#f8fafc; border:1px solid #e2e8f0; border-radius:14px;">
                <tr>
                  <td style="padding:18px 18px;">
                    <p style="margin:0 0 8px; font-size:14px; line-height:22px; color:#0f172a; font-weight:700;">
                      Important Security Note
                    </p>
                    <p style="margin:0; font-size:14px; line-height:24px; color:#64748b;">
                      Never share this OTP with anyone. Our team will never ask for your verification code by email, phone, or message.
                    </p>
                  </td>
                </tr>
              </table>

              <p style="margin:0 0 8px; font-size:14px; line-height:24px; color:#6b7280;">
                If you did not request this verification, you can safely ignore this email.
              </p>

              <p style="margin:0; font-size:14px; line-height:24px; color:#6b7280;">
                Thanks,<br />
                <strong style="color:#111827;">TBO Services Team</strong>
              </p>
            </td>
          </tr>

          <!-- Divider -->
          <tr>
            <td style="padding:0 34px;">
              <div style="height:1px; background:#e5e7eb;"></div>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:22px 34px 30px; text-align:center;">
              <p style="margin:0 0 6px; font-size:12px; line-height:20px; color:#94a3b8;">
                This is an automated message. Please do not reply directly to this email.
              </p>
              <p style="margin:0; font-size:12px; line-height:20px; color:#94a3b8;">
                © ${currentYear} TBO Services. All rights reserved.
              </p>
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
</body>
</html>
            `,
    };

    await transporter.sendMail(mailOptions);
    return true;
  } catch (error) {
    console.log("sendOtpToAuthenticatedEmail error:", error);
    return false;
  }
};

exports.logEvent = async (client, payload) => {
  try {
    await client.query(
      `
      INSERT INTO user_session_logs (
        user_id,
        session_token,
        event_type,
        platform,
        device_id,
        device_name,
        browser,
        os,
        ip_address,
        user_agent,
        personal_email,
        authenticated_email,
        message,
        meta_data,
        created_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, NOW()
      )
      `,
      [
        payload.user_id || null,
        payload.session_token || null,
        payload.event_type || null,
        payload.platform || null,
        payload.device_id || null,
        payload.device_name || null,
        payload.browser || null,
        payload.os || null,
        payload.ip_address || null,
        payload.user_agent || null,
        payload.personal_email || null,
        payload.authenticated_email || null,
        payload.message || null,
        payload.meta_data ? JSON.stringify(payload.meta_data) : null,
      ]
    );
  } catch (error) {
    console.log("logEvent error:", error.message);
  }
};

exports.createSessionAndLogin = async ({
  client,
  user,
  normalizedPlatform,
  normalizedAccess,
  generated_device_id,
  device_id,
  device_name,
  device_type,
  browser,
  os,
  ip_address,
  user_agent,
}) => {
  const tokenBundle = createEncryptedTokenForClient();
  const sessionExpiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

  await client.query(
    `
    INSERT INTO user_sessions (
      user_id,
      user_token,
      session_token,
      platform,
      device_type,
      device_id,
      generated_device_id,
      device_name,
      browser,
      os,
      ip_address,
      user_agent,
      personal_email,
      authenticated_email,
      login_type,
      login_access_used,
      is_active,
      is_logged_out,
      logout_reason,
      last_activity_at,
      logged_in_at,
      expires_at,
      created_at,
      updated_at
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
      $11, $12, $13, $14, 'PASSWORD', $15, true, false, NULL,
      NOW(), NOW(), $16, NOW(), NOW()
    )
    `,
    [
      user.id,
      user.token || null,
      tokenBundle.hashedToken,
      normalizedPlatform,
      device_type || null,
      device_id || generated_device_id || null,
      generated_device_id || null,
      device_name || null,
      browser || null,
      os || null,
      ip_address || null,
      user_agent || null,
      user.email || null,
      user.authenticated_email || null,
      normalizedAccess,
      sessionExpiresAt,
    ]
  );

  return {
    encryptedToken: tokenBundle.encryptedToken,
    expires_at: sessionExpiresAt,
    user: {
      id: user.id,
      username: user.username,
      email: user.email,
      mobile_no: user.mobile_no,
      role: user.role,
      role_id: user.role_id,
      permission_set_id: user.permission_set_id,
      email_verified: true,
    },
  };
};

exports.getMAC = () => {
  const mac = execSync('getmac').toString();
  return mac.match(/([A-Fa-f0-9]{2}[:-]){5}[A-Fa-f0-9]{2}/)[0];
}

exports.getServerTime = () => {
  return Math.floor(Date.now() / 1000);
}

exports.parseJSON = (value, fallback) => {
  try {
    return value ? JSON.parse(value) : fallback;
  } catch {
    return fallback;
  }
}

exports.getUserAgent = (req) => {
  return req.headers['user-agent'];
}

exports.getUserIp = (req) => {
  return req.headers['x-forwarded-for'] || req.connection.remoteAddress;
}

exports.amountInWords = (amount) => {
  const change_words = ['', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten', 'Eleven', 'Twelve', 'Thirteen', 'Fourteen', 'Fifteen', 'Sixteen', 'Seventeen', 'Eighteen', 'Nineteen'];
  const tens = ['', '', 'Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty', 'Seventy', 'Eighty', 'Ninety'];
  const here_digits = ['', 'Hundred', 'Thousand', 'Lakh', 'Crore'];
  let amount_after_decimal = Math.round((amount - Math.floor(amount)) * 100);
  let num = Math.floor(amount);
  let string = [], amt_hundred, x = 0;

  while (num > 0) {
    let get_divider = (x == 2) ? 10 : 100;
    let amount = Math.floor(num % get_divider);
    num = Math.floor(num / get_divider);
    x += (get_divider == 10) ? 1 : 2;
    if (amount) {
      let add_plural = ((counter = string.length) && amount > 9) ? 's' : '';
      amt_hundred = (counter == 1 && string[0]) ? ' and ' : '';
      string.push((amount < 20) ? change_words[amount] + ' ' + here_digits[counter] + add_plural + amt_hundred : tens[Math.floor(amount / 10)] + ' ' + change_words[amount % 10] + ' ' + here_digits[counter] + add_plural + amt_hundred);
    } else {
      string.push('');
    }
  }

  let implode_to_Rupees = string.reverse().join('');
  let get_paise = (amount_after_decimal > 0) ? ` And ${change_words[Math.floor(amount_after_decimal / 10)]} ${change_words[amount_after_decimal % 10]} Paise` : '';

  return implode_to_Rupees ? `${implode_to_Rupees} Rupees` : '' + get_paise;
}

// Multi Security encrypt
function hextobin(hexString) {
  let binString = '';
  for (let i = 0; i < hexString.length; i += 2) {
    binString += String.fromCharCode(parseInt(hexString.substr(i, 2), 16));
  }
  return binString;
}

exports.encrypt = (plainText, key) => {
  // Create an MD5 hash of the key (it will be 16 bytes long)
  const md5Key = crypto.createHash('md5').update(key).digest();

  // Use a fixed 16-byte initialization vector (IV)
  const initVector = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f]);

  // Create the cipher using AES-128-CBC with the 16-byte key and IV
  const cipher = crypto.createCipheriv('aes-128-cbc', md5Key, initVector);

  // Encrypt the plainText
  let encrypted = cipher.update(plainText, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  return encrypted;
}
// Multi Security decrypt

exports.decrypt = (encryptedText, key) => {
  // Create an MD5 hash of the key (it will be 16 bytes long)
  const md5Key = crypto.createHash('md5').update(key).digest(); // Directly get a Buffer

  // Use a fixed 16-byte initialization vector (IV)
  const initVector = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f]);

  // Create the decipher using AES-128-CBC with the 16-byte key and IV
  const decipher = crypto.createDecipheriv('aes-128-cbc', md5Key, initVector);

  // Decrypt the encryptedText
  let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  return decrypted;
}

exports.generateLeadNo = (lastLeadNumber = null, purpose) => {
  const today = new Date();
  const dd = String(today.getDate()).padStart(2, '0');
  const mm = String(today.getMonth() + 1).padStart(2, '0');
  const yy = String(today.getFullYear()).slice(-2);
  let serial = 1;
  if (lastLeadNumber) {
    const parts = lastLeadNumber.split('/');
    // parts = ["TYDD", "dd", "mm", "yy", "serial"]
    const lastDay = parts[1];
    const lastMonth = parts[2];
    const lastYear = parts[3];
    const lastSerial = parts[4];
    if (lastMonth === mm && lastYear === yy) {
      serial = parseInt(lastSerial) + 1;
    }
  }
  const serialStr = String(serial).padStart(2, '0');
  return `TYDD/${dd}/${mm}/${yy}/${serialStr}`;
};

exports.generateExpenseNo = (lastExpenseNo = null) => {
  const today = new Date();
  const mm = String(today.getMonth() + 1).padStart(2, '0');
  const yy = String(today.getFullYear()).slice(-2);

  let serial = 1;

  if (lastExpenseNo) {
    const parts = lastExpenseNo.split('/');
    const lastSerial = parts[2];
    const lastMonth = parts[3];
    const lastYear = parts[4];

    if (lastMonth === mm && lastYear === yy) {
      serial = parseInt(lastSerial, 10) + 1;
    }
  }
  const serialStr = String(serial).padStart(2, '0');
  return `TYDD/EXP/${serialStr}/${mm}/${yy}`;
};




