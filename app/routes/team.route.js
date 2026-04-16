const router = require('express').Router()
// const { pool } = require('../config/config');
const { authenticateToken, requirePermission, requireModulePermission, requireRole } = require('../middlewares/authenticateToken')
const teamController = require('../controllers/teams.controller.js')

router.get('/get/user/roles', [authenticateToken], teamController.getUsersRoles)

router.post('/users/parents',[authenticateToken,requirePermission('users:update')], teamController.addParentToUser);
router.get('/users/:id/members',[authenticateToken],teamController.getUserMembers);
router.delete("/users/:id/parents/:parentId",[authenticateToken,requirePermission("users:update")],teamController.deleteUserMemeber);

router.post("/hierarchy/add",[authenticateToken], teamController.addParentChild);

// remove hierarchy link
router.post("/hierarchy/remove",[authenticateToken], teamController.removeParentChild);

// get parents / children
router.get("/hierarchy/children",[authenticateToken], teamController.getChildren);
router.get("/hierarchy/parents",[authenticateToken], teamController.getParents);

// effective permissions
router.get("/hierarchy/effective-permissions",[authenticateToken], teamController.getEffectivePermissions);

// teams
router.get("/teams",[authenticateToken], teamController.getTeams);
router.post("/teams/refresh",[authenticateToken], teamController.refreshTeams);


module.exports = router