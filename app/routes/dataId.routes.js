const router = require('express').Router();
const multer = require('multer')
const { upload } = require('../middlewares/middleware.js');
const dataIdController = require('../controllers/dataId.controller.js');
const {
  authenticateToken,
  requireModulePermission
} = require('../middlewares/authenticateToken.js');

const uploadMaster = multer({ storage: multer.memoryStorage() });
// voter list base rows
router.get(
  '/get-dataid-row',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'read')
  ],
  dataIdController.getDataIdRow
);

router.get(
  '/get-dataid-all-rows',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'read')
  ],
  dataIdController.getDataIdAllActiveRows
);

router.patch(
  '/update-dataid-row',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'update')
  ],
  dataIdController.updateDataIdRow
);

// import / export
router.post(
  '/import-eroll-data',
  [
    authenticateToken,
    requireModulePermission('voter_list_import_export', 'upload'),
    upload.single('file')
  ],
  dataIdController.importErollData
);

// voter list filters / listing
router.get(
  '/voter-list-master-filter',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'read')
  ],
  dataIdController.getAllMappings
);

router.get('/autofill', dataIdController.getAutofill);

router.get(
  '/master-filter',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'read')
  ],
  dataIdController.getVotersList
);

router.get(
  '/sub-filter',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'read')
  ],
  dataIdController.getAdvancedVotersList
);

// voter list update / save
router.patch(
  '/update',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'update'),
    upload.array('photo')
  ],
  dataIdController.bulkUpdateDataIdVoters
);

// cast filter / export / register
router.get(
  '/get/wise/cast',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'cast_filter')
  ],
  dataIdController.getWiseCast
);

router.post(
  '/print/register',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'voter_register')
  ],
  dataIdController.printRegister
);

router.get(
  '/download-blank-register',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'export')
  ],
  dataIdController.downloadBlankRegister
);

router.get(
  '/get/register',
  [
    authenticateToken,
    requireModulePermission('voter_list_main', 'voter_register')
  ],
  dataIdController.getRegister
);

// data id master
router.get(
  '/get/master/tables',
  [
    authenticateToken,
    requireModulePermission('voter_list_data_id_master', 'read')
  ],
  dataIdController.getDataidImportmasterTable
);

router.delete('/delete/master/row', [authenticateToken], dataIdController.deleteMasterRow);
router.patch('/save/master/patch', [authenticateToken], dataIdController.saveMasterPatch);

router.get(
  "/download-master-excel",
  [authenticateToken, /* requireModulePermission('voter_list_data_id_master', 'download') */],
  dataIdController.downloadMasterExcel
);

router.get(
  "/download-eroll-mapping-excel",
  [authenticateToken, requireModulePermission('', 'download')],
  dataIdController.downloadErollMappingExcel
);

router.post(
  "/import-master-csv",
  [authenticateToken, uploadMaster.single("file")],
  dataIdController.importMasterCsv
);

router.post(
  '/add-row',
  [
    authenticateToken,
  ],
  dataIdController.addInDataIdImportMaster
);

router.post(
  '/add/master/row',
  [
    authenticateToken
  ],
  dataIdController.addMasterTableRow
);

router.patch(
  '/sync-dataid',
  [
    authenticateToken,
    requireModulePermission('voter_list_data_id_master', 'update')
  ],
  dataIdController.syncByDataId
);

router.delete(
  '/delete/master/tables',
  [
    authenticateToken,
    requireModulePermission('voter_list_data_id_master', 'delete')
  ],
  dataIdController.deleteRecords
);

router.post(
  '/add-empty-rows',
  [authenticateToken],
  dataIdController.addEmptyRows
)

// booth mapping
router.get(
  '/get/mapping/tables',
  [
    authenticateToken,
    requireModulePermission('voter_list_booth_mapping', 'read')
  ],
  dataIdController.getErollMappingTable
);

router.post(
  '/upload-mapping',
  [
    authenticateToken,
    requireModulePermission('voter_list_booth_mapping', 'upload'),
    upload.single('file')
  ],
  dataIdController.uploadMappingOverride
);

router.post(
  '/sync-mapping',
  [
    authenticateToken,
    requireModulePermission('voter_list_booth_mapping', 'update')
  ],
  dataIdController.syncMappingToEroll
);

router.get(
  '/download-mapping',
  [
    authenticateToken,
    requireModulePermission('voter_list_booth_mapping', 'download')
  ],
  dataIdController.downloadMappingExcel
);

router.post(
  "/add-empty-row",
  [
    authenticateToken,
    requireModulePermission("dataid_importmaster", "create"),
  ],
  dataIdController.addEmptyImportMasterRow
);

router.patch(
  '/mapping-to-db',
  [
    authenticateToken,
    requireModulePermission('voter_list_booth_mapping', 'update')
  ],
  dataIdController.updateMapping
);

router.patch(
  '/db-to-mapping',
  [
    authenticateToken,
    requireModulePermission('voter_list_booth_mapping', 'update')
  ],
  dataIdController.updateMappingFromDb
);

// other master
router.post(
  '/generate/surnames',
  [
    authenticateToken,
    requireModulePermission('voter_list_other_master', 'update')
  ],
  dataIdController.generateSurname
);

router.post(
  '/generate/ids',
  [
    authenticateToken,
    requireModulePermission('voter_list_other_master', 'update')
  ],
  dataIdController.generateMappingids
);

router.post(
  '/generate/familyid',
  [
    authenticateToken,
    requireModulePermission('voter_list_other_master', 'update')
  ],
  dataIdController.generateFamilyIds
);

// yojna master
router.post(
  '/yojna/list',
  [
    authenticateToken,
    requireModulePermission('voter_list_yojna_master', 'read')
  ],
  dataIdController.getYojnaList
);

router.post(
  '/sync/surname',
  [
    authenticateToken
  ],
  dataIdController.syncSurname
)

module.exports = router;