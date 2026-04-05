const { GetUsers } = require('../controllers/Dashboard');

module.exports = async function (req, res, next) {
    let session = req.session.user;

    if (session) {
        const userData = await GetUsers(req, res);
        if (!userData || userData.length === 0) {
            return res.status(404).send('User not found');
        }
        else if (userData[0].profile_status == 0) {
            return res.redirect('/profile');
        }
        next();
    } else { 
    res.redirect('/login');
    }
};
