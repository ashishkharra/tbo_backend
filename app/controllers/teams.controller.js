const { pool } = require('../config/config.js');
const {
  getActiveUserById,
  checkExistingParentRelation,
  insertParentRelation,
  updateUserPermissions,
  syncPermissionsToParent,
  getMembersByParentId,
  countMembersByParentId,
  removeParentRelationship,
  syncPermissionsFromParent,
  addParentChildLink,
  removeParentChildLink,
  getDirectChildren,
  getDirectParents,
  getAllChildren,
  getAllParents,
  getEffectivePermissions,
  getEffectivePermissionsGrouped,
  refreshTeams,
  getTeams,
  getUsersRoles
} = require('../models/team.model.js');


module.exports = {

  getUsersRoles: async (req, res) => {
    try {
      const result = await getUsersRoles(req.user, {
        search: req.query.search || "",
        role: req.query.role || "",
        limit: req.query.limit || 50,
      });

      return res.status(200).json({
        success: true,
        message: "Users roles fetched successfully",
        data: result,
      });
    } catch (error) {
      console.error("getUsersRoles error:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to fetch users roles",
        error: error.message,
      });
    }
  },

  addParentToUser: async (req, res) => {
    try {
      const userId = Number(req.body.userId);
      const parentId = Number(req.body.parent_id);

      if (!parentId) {
        return res.status(400).json({
          success: false,
          message: 'parent_id is required'
        });
      }

      if (userId === parentId) {
        return res.status(400).json({
          success: false,
          message: 'User cannot be their own parent'
        });
      }

      const userCheck = await pool.query(
        `SELECT id FROM users WHERE id = $1 AND is_active = true`,
        [userId]
      );

      if (!userCheck.rows.length) {
        return res.status(404).json({
          success: false,
          message: 'User not found or inactive'
        });
      }

      const parentCheck = await pool.query(
        `SELECT id FROM users WHERE id = $1 AND is_active = true`,
        [parentId]
      );

      if (!parentCheck.rows.length) {
        return res.status(404).json({
          success: false,
          message: 'Parent user not found or inactive'
        });
      }

      const exists = await pool.query(
        `SELECT 1 FROM user_parents 
         WHERE user_id = $1 AND parent_id = $2 AND is_active = true`,
        [userId, parentId]
      );

      if (exists.rows.length) {
        return res.status(400).json({
          success: false,
          message: 'Parent relationship already exists'
        });
      }

      await pool.query(
        `INSERT INTO user_parents (user_id, parent_id, assigned_by, is_active)
         VALUES ($1, $2, $3, true)`,
        [userId, parentId, req.user.id]
      );

      await syncPermissionsToParent(userId, parentId);

      res.json({
        success: true,
        message: 'Parent relationship added successfully'
      });

    } catch (err) {
      console.error(err);
      res.status(500).json({
        success: false,
        message: 'Failed to add parent relationship'
      });
    }
  },

  getUserMembers: async (req, res) => {
    try {
      const parentId = Number(req.params.id);
      const requester = req.user;

      if (!parentId || Number.isNaN(parentId)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid user id'
        });
      }

      if (requester.role !== 'admin' && requester.id !== parentId) {
        return res.status(403).json({
          success: false,
          message: 'You are not allowed to view these members'
        });
      }

      const page = Math.max(parseInt(req.query.page) || 1, 1);
      const limit = Math.min(parseInt(req.query.limit) || 20, 100);
      const offset = (page - 1) * limit;

      const [members, total] = await Promise.all([
        getMembersByParentId(parentId, limit, offset),
        countMembersByParentId(parentId)
      ]);

      const data = members.map(row => ({
        id: row.user_id,
        username: row.username,
        email: row.email,
        mobile: row.mobile,
        role: row.role,
        assignedModules: row.assigned_modules
          ? JSON.parse(row.assigned_modules)
          : [],
        relationship_id: row.relationship_id,
        assigned_at: row.assigned_at,
        assigned_by: row.assigned_by,
        sub_members: Array.isArray(row.sub_members)
          ? row.sub_members
          : []
      }));

      res.json({
        success: true,
        meta: {
          page,
          limit,
          total,
          totalPages: Math.ceil(total / limit)
        },
        data
      });

    } catch (error) {
      console.error('Error fetching user members:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to fetch user members'
      });
    }
  },

  deleteUserMemeber: async (req, res) => {
    try {
      const { id, parentId } = req.params;
      await removeParentRelationship(id, parentId);
      await syncPermissionsFromParent(id, parentId);

      res.json({
        success: true,
        message: "Parent relationship removed successfully"
      });
    } catch (error) {
      console.error("Error removing parent relationship:", error);
      res.status(500).json({
        success: false,
        message: "Failed to remove parent relationship"
      });
    }
  },

  addParentChild: async (req, res) => {
    try {
      const result = await addParentChildLink(req.body);

      if (!result.success) {
        return res.status(400).json(result);
      }

      return res.status(200).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

  removeParentChild: async (req, res) => {
    try {
      const result = await removeParentChildLink(req.body);

      if (!result.success) {
        return res.status(400).json(result);
      }

      return res.status(200).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

  getChildren: async (req, res) => {
    try {
      const { user_id, recursive } = req.query;

      const result = String(recursive) === "true"
        ? await getAllChildren(user_id)
        : await getDirectChildren(user_id);

      return res.status(result.success ? 200 : 400).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

  getParents: async (req, res) => {
    try {
      const { user_id, recursive } = req.query;

      const result = String(recursive) === "true"
        ? await getAllParents(user_id)
        : await getDirectParents(user_id);

      return res.status(result.success ? 200 : 400).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

  getEffectivePermissions: async (req, res) => {
    try {
      const { user_id, grouped } = req.query;

      const result = String(grouped) === "true"
        ? await getEffectivePermissionsGrouped(user_id)
        : await getEffectivePermissions(user_id);

      return res.status(result.success ? 200 : 400).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

  refreshTeams: async (req, res) => {
    try {
      const result = await refreshTeams();
      return res.status(result.success ? 200 : 400).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

  getTeams: async (req, res) => {
    try {
      const result = await getTeams(req.query);
      return res.status(result.success ? 200 : 400).json(result);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || "Internal server error"
      });
    }
  },

};
