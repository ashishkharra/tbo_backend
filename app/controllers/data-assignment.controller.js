const dataAssignmentModel = require('../models/data-assignment.model')

const dataAssignmentController = {
  getMeta: async (req, res) => {
    try {
      const result = await dataAssignmentModel.getMeta();

      return res.status(200).json({
        success: true,
        message: 'Data assignment meta fetched successfully',
        data: result
      });
    } catch (error) {
      console.error('getMeta error:', error);
      return res.status(500).json({
        success: false,
        message: 'Something went wrong'
      });
    }
  },

  getOptions: async (req, res) => {
    try {
      const query = req.query || {};
      const body = req.body || {};

      const payload = {
        ...query,
        ...body,
        data_ids:
          query.data_ids ??
          query['data_ids[]'] ??
          body.data_ids ??
          body['data_ids[]'] ??
          query.data_id ??
          body.data_id ??
          null
      };

      const result = await dataAssignmentModel.getOptions(payload);

      return res.status(200).json({
        success: true,
        message: 'Options fetched successfully',
        data: result
      });
    } catch (error) {
      console.log('getOptions controller error =>', error);
      return res.status(500).json({
        success: false,
        message: error.message || 'Something went wrong',
        data: {}
      });
    }
  },

  getUserAssignments: async (req, res) => {
    try {
      const user_id = req.params.user_id || null;
      const requesterRole = req.user?.role || null;
      const selected_role_id = req.query.role_id || null;
      const currentUserId = req.user.id ||  null

      if (!user_id) {
        return res.status(400).json({
          success: false,
          message: 'user_id is required'
        });
      }

      const result = await dataAssignmentModel.getUserAssignments(
        user_id,
        requesterRole,
        selected_role_id,
        currentUserId
      );

      return res.status(200).json({
        success: true,
        message: 'User assignments fetched successfully',
        data: result
      });
    } catch (error) {
      console.error('getUserAssignments error:', error);
      return res.status(500).json({
        success: false,
        message: error.message || 'Something went wrong'
      });
    }
  },

  getPreview: async (req, res) => {
    try {
      const { table, wise_type, data_id, wise_value_id } = req.body;

      if (!table) {
        return res.status(400).json({
          success: false,
          message: 'table is required'
        });
      }

      if (!wise_type) {
        return res.status(400).json({
          success: false,
          message: 'wise_type is required'
        });
      }

      if (!data_id) {
        return res.status(400).json({
          success: false,
          message: 'data_id is required'
        });
      }

      const result = await dataAssignmentModel.getPreview({
        table,
        wise_type,
        data_id,
        wise_value_id
      });

      return res.status(200).json({
        success: true,
        message: 'Preview fetched successfully',
        data: result
      });
    } catch (error) {
      console.error('getPreview error:', error);

      return res.status(500).json({
        success: false,
        message: error.message || 'Something went wrong'
      });
    }
  },

  saveAssignments: async (req, res) => {
    try {
      const { user_id, data_assignments, column_permissions } = req.body;
      const updatedBy = req.user?.id || null;

      if (!user_id) {
        return res.status(400).json({
          success: false,
          message: 'user_id is required'
        });
      }

      if (!Array.isArray(data_assignments)) {
        return res.status(400).json({
          success: false,
          message: 'data_assignments must be an array'
        });
      }

      if (!Array.isArray(column_permissions)) {
        return res.status(400).json({
          success: false,
          message: 'column_permissions must be an array'
        });
      }

      // Process data_assignments to ensure proper field mapping
      const processedAssignments = data_assignments.map(item => ({
        ...item,
        // Map frontend fields to database fields
        age_from: item.ageFrom || item.age_from,
        age_to: item.ageTo || item.age_to,
        cast_filter: item.cast || item.cast_filter,
        // Ensure numeric values
        age_from: item.ageFrom ? parseInt(item.ageFrom) : (item.age_from ? parseInt(item.age_from) : null),
        age_to: item.ageTo ? parseInt(item.ageTo) : (item.age_to ? parseInt(item.age_to) : null)
      }));

      // Validate column permissions
      for (let i = 0; i < column_permissions.length; i++) {
        const col = column_permissions[i];

        const can_view = !!col.can_view;
        const can_mask = !!col.can_mask;
        const can_edit = !!col.can_edit;
        const can_copy = !!col.can_copy;

        if (can_mask && can_edit) {
          return res.status(400).json({
            success: false,
            message: `Column '${col.column_name}' cannot have both mask and edit permission`
          });
        }

        if ((can_edit || can_mask) && !can_view) {
          col.can_view = true;
        }

        col.can_mask = can_mask;
        col.can_edit = can_edit;
        col.can_copy = can_copy;
      }

      const result = await dataAssignmentModel.saveAssignments({
        user_id,
        data_assignments: processedAssignments,
        column_permissions,
        updated_by: updatedBy
      });

      return res.status(200).json({
        success: true,
        message: 'User assignments saved successfully',
        data: result
      });
    } catch (error) {
      console.error('saveAssignments error:', error);
      return res.status(500).json({
        success: false,
        message: error.message || 'Something went wrong'
      });
    }
  },

  applyAccessCodesToUser: async (req, res) => {
    try {
      const { user_id, modules_code, permission_code } = req.body;
      const updated_by = req.user?.id || null;

      if (!user_id) {
        return res.status(400).json({
          success: false,
          message: "user_id is required"
        });
      }

      if (!modules_code || !String(modules_code).trim()) {
        return res.status(400).json({
          success: false,
          message: "modules_code is required"
        });
      }

      if (!permission_code || !String(permission_code).trim()) {
        return res.status(400).json({
          success: false,
          message: "permission_code is required"
        });
      }

      const result = await dataAssignmentModel.applyAccessCodesToUser({
        user_id: Number(user_id),
        modules_code: String(modules_code).trim(),
        permission_code: String(permission_code).trim(),
        updated_by
      });

      return res.status(200).json({
        success: true,
        message: "Access codes applied successfully",
        data: result
      });
    } catch (error) {
      console.error("applyAccessCodesToUser error:", error);
      return res.status(500).json({
        success: false,
        message: error.message || "Something went wrong"
      });
    }
  },

}

module.exports = dataAssignmentController;  