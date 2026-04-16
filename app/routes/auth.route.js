const express = require('express');
const router = express.Router();
const authController = require('../controllers/auth.controller');
const { authenticateToken, requireModulePermission } = require('../middlewares/authenticateToken')

router.post("/initiate-login", authController.initiateLogin);
router.post("/verify-device-otp", authController.verifyDeviceOtp);
router.post("/verify-user-otp", authController.verifyUserOtp);
router.post('/logout', [authenticateToken], authController.logout)
router.get("/users/:id/details",[authenticateToken, requireModulePermission('tbo_users_users', 'read')],authController.getUserDetails);
router.patch("/users/bulk-update",[authenticateToken, requireModulePermission('tbo_users_users', 'update')],authController.bulkUpdateUsers);
router.get('/users', [authenticateToken, requireModulePermission('tbo_users_list', 'read')], authController.getUsers);
router.post('/users', [authenticateToken, requireModulePermission('tbo_users_users', 'create')], authController.postUser)
router.patch('/users/:id', [authenticateToken, requireModulePermission('tbo_users_users', 'update')], authController.updateUser)
router.delete('/users', [authenticateToken, requireModulePermission('tbo_users_users', 'delete')], authController.deleteUsers);
router.put('/users/:id/password', [authenticateToken, requireModulePermission('tbo_users_users', 'update')], authController.updateUserPassword);
router.put('/users/:id/change-password', [authenticateToken], authController.changeOwnPassword);
router.get("/modules-code", [authenticateToken, requireModulePermission('tbo_users_users', 'read')], authController.getModulesCode);
router.get('/profile', [authenticateToken], authController.getProfile);

router.patch('/unlink/:id/user', [authenticateToken, requireModulePermission('tbo_users_users', 'update')], authController.unlinkUser)


router.post("/users/assign-modules",[authenticateToken, requireModulePermission('tbo_users_users','update')], authController.assignModulesToUser);
router.post("/users/apply-modules-code",[authenticateToken, requireModulePermission('tbo_users_users','update')], authController.applyModulesCodeToUser);
router.get("/users/:userId/modules",[authenticateToken, requireModulePermission('tbo_users_users','read')], authController.getUserAssignedModules);


router.get("/user-permissions/:userId",[authenticateToken, requireModulePermission('tbo_users_users', 'read')], authController.getUserPermissions);
router.get('/get/permission/modules', [authenticateToken, requireModulePermission('tbo_users_users', 'read')], authController.getPermissionModules)
router.get('/get/table/columns', [authenticateToken, requireModulePermission('tbo_users_users', 'read')], authController.getTableColumns)
router.get('/get/assignable/data', [authenticateToken, requireModulePermission('data_set_master', 'read')], authController.getAssignableData)

module.exports = router;