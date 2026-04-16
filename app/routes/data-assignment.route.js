const express = require('express');
const router = express.Router();
const dataAssignmentController = require('../controllers/data-assignment.controller');
const { authenticateToken, requireModulePermission } = require('../middlewares/authenticateToken')

router.get('/meta', [authenticateToken], dataAssignmentController.getMeta);
router.get('/options', [authenticateToken], dataAssignmentController.getOptions);
router.get('/details', [authenticateToken], dataAssignmentController.getUserAssignments);
router.get('/details/:user_id', [authenticateToken], dataAssignmentController.getUserAssignments);
router.post('/preview', [authenticateToken], dataAssignmentController.getPreview);

router.post('/save', [authenticateToken], dataAssignmentController.saveAssignments);
router.post(
  "/apply-access-codes",
  [authenticateToken, requireModulePermission("tbo_users", "update")],
  dataAssignmentController.applyAccessCodesToUser
);

// router.get('/:user_id', [authenticateToken, requireModulePermission('tbo_users_users','read')], dataAssignmentController.getUserAssignments);
module.exports = router;