const multer = require("multer");
const XLSX = require("xlsx");
const path = require('path')

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
      cb(null, path.join(process.cwd(), "public", "uploads"));
    },
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname);
      const name = Date.now() + "-" + Math.round(Math.random() * 1e9);
      cb(null, name + ext);
    }
});
  
const upload = multer({ storage });

const userData = async (req, res, next) => {
  try {
    
    const publicRoutesRegex = [
      /^\/login$/,                   
      /^\/logout$/,                  
      /^\/forgot-password$/,         
      /^\/reset-password(\/.*)?$/    
    ];
    if (
      publicRoutesRegex.some(routeRegex => routeRegex.test(req.path)) ||
      req.path.startsWith('/api/')
    ) {
      return next();
    }
    const token = req.session?.user?.token;
    if (!token) {
      return res.redirect('/login');
    }

    // ✅ admin data verify
    const adminData = await getAdminDataByToken(token);
    if (!adminData || adminData.length === 0) {
      return res.status(404).send("Admin data not found");
    }

    // ✅ ejs ke liye global admin
    res.locals.admin = adminData[0];
    next();

  } catch (error) {
    console.error("Middleware error:", error);
    res.status(500).send("Internal Server Error");
  }
};

module.exports = { userData, upload };
