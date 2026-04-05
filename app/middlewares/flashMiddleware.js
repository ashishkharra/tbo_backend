module.exports = function (req, res, next) {
  // Initialize flash object safely
  if (!req.session.flash) {
    req.session.flash = {};
  }

  // Method to set flash messages
  req.setFlash = (type, message) => {
    if (!req.session.flash) {
      req.session.flash = {}; // Double safety
    }
    req.session.flash[type] = message;
  };

  // Method to get flash messages
  res.getFlash = (type) => {
    if (!req.session.flash) return null;
    const message = req.session.flash[type];
    delete req.session.flash[type];
    return message;
  };

  next();
};
