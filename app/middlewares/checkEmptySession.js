module.exports = checkEmptySession  = async(req, res, next) => {
    if (!req.session || !req.session.user) {
        return res.redirect('/login');
      }else{
        return next();
      }
  }
  