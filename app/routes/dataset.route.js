const router = require('express').Router();
const datasetController = require('../controllers/dataset.controller');
const { authenticateToken, requireModulePermission } = require('../middlewares/authenticateToken.js');
const { upload } = require('../middlewares/middleware.js');

router.post(
  '/import',
  [authenticateToken, requireModulePermission('data_set_import_export', 'import')],
  upload.single('file'),
  datasetController.importFile
);

router.get(
  '/import-history',
  [authenticateToken, requireModulePermission('data_set_import_export', 'read')],
  datasetController.importHistory
);

router.get(
  '/',
  [authenticateToken, requireModulePermission('data_set_main', 'read')],
  datasetController.getDataSet
);

router.get(
  '/area-mapping',
  [authenticateToken],
  datasetController.getAreaMapping
);

router.get(
  '/master-filter',
  [authenticateToken, requireModulePermission('data_set_main', 'read')],
  datasetController.fetchOptions
);

router.get(
  '/voters',
  [authenticateToken, requireModulePermission('data_set_main', 'read')],
  datasetController.fetchTableData
);

router.get(
  '/sub-filter',
  [authenticateToken, requireModulePermission('data_set_main', 'read')],
  datasetController.filterFetchedTableData
);

router.patch(
  '/update',
  [authenticateToken, requireModulePermission('data_set_main', 'update')],
  datasetController.updateDataSet
);

router.delete(
  '/delete',
  [authenticateToken, requireModulePermission('data_set_main', 'delete')],
  datasetController.deleteDataSet
);

module.exports = router;