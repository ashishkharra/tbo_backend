module.exports = isAuthenticated = async(req, res, next) => {
    if (req.session && req.session.user) {
        return res.redirect('/');
      }else{
        return next();
      }
}
