const router = require('express').Router();
const {
    authenticateToken,
    requireModulePermission
} = require('../middlewares/authenticateToken');

const accessMap = {
    voter_list: { moduleCode: 'voter_list_main', actionCode: 'read' },
    voter_list_edit: { moduleCode: 'voter_list_main', actionCode: 'update' },
    voter_import: { moduleCode: 'voter_list_import_export', actionCode: 'upload' },
    cast_filter: { moduleCode: 'voter_list_main', actionCode: 'cast_filter' },
    voter_register: { moduleCode: 'voter_list_main', actionCode: 'voter_register' },
    blank_register_export: { moduleCode: 'voter_list_main', actionCode: 'export' },

    data_id_master: { moduleCode: 'voter_list_data_id_master', actionCode: 'read' },
    booth_mapping: { moduleCode: 'voter_list_booth_mapping', actionCode: 'read' },
    other_master: { moduleCode: 'voter_list_other_master', actionCode: 'update' },
    yojna_master: { moduleCode: 'voter_list_yojna_master', actionCode: 'read' },

    tbo_users: { moduleCode: 'tbo_users_list', actionCode: 'read' },

    dataset: { moduleCode: 'data_set_main', actionCode: 'read' },

    data_set_import: { moduleCode: 'data_set_import_export', actionCode: 'import' },
    data_set_import_history: { moduleCode: 'data_set_import_export', actionCode: 'read' },
    data_set_export_file: { moduleCode: 'data_set_import_export', actionCode: 'export' },
};

const menuKeys = ["voter_list", "tbo_users", "data_set"];
const assignedModules = [
    "voter_list_main",
    "voter_list_master",
    "voter_list_data_id_master",
    "voter_list_cast_id_master",
    "voter_list_other_master",
    "voter_list_yojna_master",
    "voter_list_booth_mapping",
    "tbo_users_list",
    "data_set_main",
    "data_set_update",
    "data_set_export"
];

router.get('/:key', authenticateToken, (req, res, next) => {
    const rawKey = req.params.key;
    const normalizedKey = rawKey
        .trim()
        .toLowerCase()
        .replace(/-/g, '_')
        .replace(/\s+/g, '_');

    const user = req.user;

    if (
        user?.role === 'admin' ||
        user?.role === 'super_admin' ||
        user?.role === 'Admin' ||
        user?.role === 'Super Admin'
    ) {
        return res.status(200).json({
            success: true,
            allowed: true,
            message: 'Access granted (admin bypass)'
        });
    }

    const config = accessMap[normalizedKey];

    if (!config) {
        return res.status(404).json({
            success: false,
            allowed: false,
            message: `Invalid access key: ${rawKey}`
        });
    }

    const middleware = requireModulePermission(
        config.moduleCode,
        config.actionCode
    );

    return middleware(req, res, () => {
        return res.status(200).json({
            success: true,
            allowed: true,
            message: 'Access granted',
            module: config.moduleCode,
            action: config.actionCode
        });
    });
});

module.exports = router;