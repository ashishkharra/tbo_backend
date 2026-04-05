const router = require('express').Router()
// const { pool } = require('../config/config');
const { authenticateToken, requirePermission, requireRole } = require('../middlewares/authenticateToken')
const teamController = require('../controllers/teams.controller.js')

/**
 * POST /api/teams
 * Create a new team
 * Body: { name, description, parent_id, team_code }
 */
// router.post('/', authenticateToken, requirePermission('teams:create'), async (req, res) => {
//     try {
//         const { name, description, parent_id, team_code } = req.body;

//         if (!name || !name.trim()) {
//             return res.status(400).json({
//                 success: false,
//                 message: 'Team name is required'
//             });
//         }

//         if (team_code) {
//             const { rowCount } = await pool.query(
//                 'SELECT 1 FROM team_master WHERE team_code = $1',
//                 [team_code]
//             );

//             if (rowCount > 0) {
//                 return res.status(400).json({
//                     success: false,
//                     message: 'Team code already exists'
//                 });
//             }
//         }

//         let hierarchy_level = 0;
//         let hierarchy_path = null;
//         let parentId = null;

//         if (parent_id !== undefined && parent_id !== null) {
//             parentId = Number(parent_id);

//             const parentResult = await pool.query(
//                 'SELECT hierarchy_level, hierarchy_path FROM team_master WHERE id = $1::int',
//                 [parentId]
//             );

//             if (parentResult.rowCount === 0) {
//                 return res.status(400).json({
//                     success: false,
//                     message: 'Invalid parent team'
//                 });
//             }

//             const parent = parentResult.rows[0];
//             hierarchy_level = parent.hierarchy_level + 1;
//             hierarchy_path = parent.hierarchy_path
//                 ? `${parent.hierarchy_path}/${parentId}`
//                 : `${parentId}`;
//         }

//         const { rows } = await pool.query(
//             `
//         INSERT INTO team_master
//         (
//           name,
//           description,
//           parent_id,
//           team_code,
//           hierarchy_level,
//           hierarchy_path,
//           is_active,
//           created_by
//         )
//         VALUES
//         ($1, $2, $3, $4, $5, $6, TRUE, $7)
//         RETURNING *
//         `,
//             [
//                 name.trim(),
//                 description || null,
//                 parentId,
//                 team_code || null,
//                 hierarchy_level,
//                 hierarchy_path,
//                 req.user.id
//             ]
//         );

//         res.status(201).json({
//             success: true,
//             message: 'Team created successfully',
//             data: rows[0]
//         });
//     } catch (error) {
//         console.log('Team creation error : ', error)
//         res.status(500).json({
//             success: false,
//             message: 'Failed to create team'
//         });
//     }
// });

// /**
//  * GET /api/teams
//  * Get all teams (with hierarchy information)
//  * Query params: parent_id (optional), is_active (optional)
//  */
// router.get('/', requireRole(['super_admin', 'admin', 'leader']), async (req, res) => {
//     try {
//         const { parent_id, is_active } = req.query;

//         let sql = `
//         SELECT
//           tm.id,
//           tm.name,
//           tm.description,
//           tm.parent_id,
//           tm.team_code,
//           tm.hierarchy_level,
//           tm.hierarchy_path,
//           tm.is_active,
//           tm.created_by,
//           tm.token,
//           tm.created_at,
//           tm.updated_at,
//           parent.name AS parent_name,
//           parent.team_code AS parent_team_code,
//           creator.username AS created_by_username,
//           (
//             SELECT COUNT(*)::int
//             FROM users u
//             WHERE u.team_id = tm.id::text
//               AND u.is_active = TRUE
//           ) AS user_count,
//           (
//             SELECT COUNT(*)::int
//             FROM team_master c
//             WHERE c.parent_id = tm.id
//               AND c.is_active = TRUE
//           ) AS child_team_count
//         FROM team_master tm
//         LEFT JOIN team_master parent ON parent.id = tm.parent_id
//         LEFT JOIN users creator ON creator.id = tm.created_by
//       `;

//         const conditions = [];
//         const params = [];

//         if (parent_id !== undefined) {
//             if (parent_id === 'null') {
//                 conditions.push(`tm.parent_id IS NULL`);
//             } else {
//                 params.push(Number(parent_id));
//                 conditions.push(`tm.parent_id = $${params.length}::int`);
//             }
//         }

//         if (is_active !== undefined) {
//             params.push(is_active === 'true');
//             conditions.push(`tm.is_active = $${params.length}::boolean`);
//         }

//         if (conditions.length) {
//             sql += ` WHERE ${conditions.join(' AND ')}`;
//         }

//         sql += ` ORDER BY tm.hierarchy_level, tm.name`;

//         const { rows } = await pool.query(sql, params);

//         res.status(200).json({
//             success: true,
//             data: rows
//         });
//     } catch (error) {
//         console.log(error)
//         res.status(500).json({
//             success: false,
//             message: 'Failed to fetch teams'
//         });
//     }
// });

// /**
//  * GET /api/teams/:id
//  * Get team details with users and child teams
//  */
// router.get('/:id', /*authenticateToken,*/ requireRole(['super_admin', 'admin', 'leader']), async (req, res) => {
//     try {
//         const id = Number(req.params.id);
//         console.log('id ->>> ', typeof(id))

//         // if (!Number.isInteger(id)) {
//         //     return res.status(400).json({
//         //         success: false,
//         //         message: 'Invalid team id'
//         //     });
//         // }

//         const teamResult = await pool.query(
//             `
//             SELECT
//               tm.*,
//               parent.name AS parent_name,
//               parent.team_code AS parent_team_code,
//               creator.username AS created_by_username
//             FROM team_master tm
//             LEFT JOIN team_master parent ON parent.id = tm.parent_id
//             LEFT JOIN users creator ON creator.id = tm.created_by
//             WHERE tm.id = $1
//             `,
//             [id]
//         );

//         if (teamResult.rowCount === 0) {
//             return res.status(404).json({
//                 success: false,
//                 message: 'Team not found'
//             });
//         }

//         const team = teamResult.rows[0];

//         const usersResult = await pool.query(
//             `
//             SELECT
//               id,
//               username,
//               email,
//               mobile_no AS mobile,
//               role,
//               parent_id,
//               is_active,
//               created_at
//             FROM users
//             WHERE team_id = $1
//               AND is_active = TRUE
//             ORDER BY username
//             `,
//             [id]
//         );

//         const childTeamsResult = await pool.query(
//             `
//             SELECT
//               tm.id,
//               tm.name,
//               tm.description,
//               tm.team_code,
//               tm.hierarchy_level,
//               (
//                 SELECT COUNT(*)
//                 FROM users u
//                 WHERE u.team_id = tm.id
//                   AND u.is_active = TRUE
//               ) AS user_count
//             FROM team_master tm
//             WHERE tm.parent_id = $1
//               AND tm.is_active = TRUE
//             ORDER BY tm.name
//             `,
//             [id]
//         );

//         res.json({
//             success: true,
//             data: {
//                 id: team.id,
//                 name: team.name,
//                 description: team.description,
//                 parent_id: team.parent_id,
//                 parent_name: team.parent_name,
//                 parent_team_code: team.parent_team_code,
//                 team_code: team.team_code,
//                 hierarchy_level: team.hierarchy_level,
//                 hierarchy_path: team.hierarchy_path,
//                 is_active: team.is_active,
//                 created_by: team.created_by,
//                 created_by_username: team.created_by_username,
//                 created_at: team.created_at,
//                 updated_at: team.updated_at,
//                 users: usersResult.rows,
//                 child_teams: childTeamsResult.rows.map(ct => ({
//                     ...ct,
//                     user_count: Number(ct.user_count) || 0
//                 }))
//             }
//         });

//     } catch (error) {
//         console.error('Error fetching team:', error);
//         res.status(500).json({
//             success: false,
//             message: 'Failed to fetch team'
//         });
//     }
// });

// /**
//  * PUT /api/teams/:id
//  * Update team
//  */
// router.put('/:id', authenticateToken, requireRole(['super_admin', 'admin']), async (req, res) => {
//     try {
//         const { id } = req.params;
//         const { name, description, parent_id, team_code, is_active } = req.body;

//         const existingTeamResult = await pool.query(
//             `SELECT * FROM team_master WHERE id = $1`,
//             [id]
//         );

//         if (existingTeamResult.rowCount === 0) {
//             return res.status(404).json({
//                 success: false,
//                 message: 'Team not found'
//             });
//         }

//         const existingTeam = existingTeamResult.rows[0];

//         if (team_code && team_code !== existingTeam.team_code) {
//             const codeCheck = await pool.query(
//                 `SELECT id FROM team_master WHERE team_code = $1 AND id != $2`,
//                 [team_code, id]
//             );

//             if (codeCheck.rowCount > 0) {
//                 return res.status(400).json({
//                     success: false,
//                     message: 'Team code already exists'
//                 });
//             }
//         }

//         let hierarchyLevel = existingTeam.hierarchy_level;
//         let hierarchyPath = existingTeam.hierarchy_path;

//         if (
//             parent_id !== undefined &&
//             parent_id !== existingTeam.parent_id
//         ) {
//             if (parent_id !== null && Number(parent_id) === Number(id)) {
//                 return res.status(400).json({
//                     success: false,
//                     message: 'Team cannot be its own parent'
//                 });
//             }

//             if (parent_id) {
//                 const currentPath = existingTeam.hierarchy_path || '';
//                 const pathParts = currentPath.split('/').filter(Boolean);

//                 if (pathParts.includes(String(parent_id))) {
//                     return res.status(400).json({
//                         success: false,
//                         message: 'Cannot move team under its own descendant'
//                     });
//                 }
//             }

//             if (parent_id) {
//                 const parentResult = await pool.query(
//                     `SELECT hierarchy_level, hierarchy_path
//              FROM team_master
//              WHERE id = $1`,
//                     [parent_id]
//                 );

//                 if (parentResult.rowCount === 0) {
//                     return res.status(400).json({
//                         success: false,
//                         message: 'Invalid parent team'
//                     });
//                 }

//                 const parent = parentResult.rows[0];
//                 hierarchyLevel = parent.hierarchy_level + 1;
//                 hierarchyPath = `${parent.hierarchy_path || ''}/${parent_id}`;
//             } else {
//                 hierarchyLevel = 0;
//                 hierarchyPath = null;
//             }
//         }

//         const fields = [];
//         const values = [];
//         let index = 1;

//         if (name !== undefined) {
//             fields.push(`name = $${index++}`);
//             values.push(name.trim());
//         }

//         if (description !== undefined) {
//             fields.push(`description = $${index++}`);
//             values.push(description);
//         }

//         if (parent_id !== undefined) {
//             fields.push(`parent_id = $${index++}`);
//             fields.push(`hierarchy_level = $${index++}`);
//             fields.push(`hierarchy_path = $${index++}`);
//             values.push(parent_id || null, hierarchyLevel, hierarchyPath);
//         }

//         if (team_code !== undefined) {
//             fields.push(`team_code = $${index++}`);
//             values.push(team_code || null);
//         }

//         if (is_active !== undefined) {
//             fields.push(`is_active = $${index++}`);
//             values.push(Boolean(is_active));
//         }

//         if (fields.length === 0) {
//             return res.status(400).json({
//                 success: false,
//                 message: 'No fields to update'
//             });
//         }

//         values.push(id);

//         await pool.query(
//             `UPDATE team_master
//          SET ${fields.join(', ')}
//          WHERE id = $${index}`,
//             values
//         );

//         const updatedTeamResult = await pool.query(
//             `SELECT * FROM team_master WHERE id = $1`,
//             [id]
//         );

//         res.json({
//             success: true,
//             message: 'Team updated successfully',
//             data: updatedTeamResult.rows[0]
//         });
//     } catch (error) {
//         console.error('Error updating team:', error);
//         res.status(500).json({
//             success: false,
//             message: error.message || 'Failed to update team'
//         });
//     }
// });

// /**
//  * DELETE /api/teams/:id
//  * Delete team (only if no users and no child teams)
//  */
// router.delete('/:id', authenticateToken, requireRole(['super_admin', 'admin']), async (req, res) => {
//     try {
//         const { id } = req.params;

//         const teamResult = await pool.query(
//             `SELECT id FROM team_master WHERE id = $1`,
//             [id]
//         );

//         if (teamResult.rowCount === 0) {
//             return res.status(404).json({
//                 success: false,
//                 message: 'Team not found'
//             });
//         }

//         const usersExist = await pool.query(
//             `SELECT 1
//          FROM users
//          WHERE team_id = $1
//            AND isActive = true
//          LIMIT 1`,
//             [id]
//         );

//         if (usersExist.rowCount > 0) {
//             return res.status(400).json({
//                 success: false,
//                 message:
//                     'Cannot delete team with active users. Please reassign users first.'
//             });
//         }

//         const childTeamsExist = await pool.query(
//             `SELECT 1
//          FROM team_master
//          WHERE parent_id = $1
//            AND is_active = true
//          LIMIT 1`,
//             [id]
//         );

//         if (childTeamsExist.rowCount > 0) {
//             return res.status(400).json({
//                 success: false,
//                 message:
//                     'Cannot delete team with child teams. Please delete or reassign child teams first.'
//             });
//         }

//         await pool.query(
//             `DELETE FROM team_master WHERE id = $1`,
//             [id]
//         );

//         res.json({
//             success: true,
//             message: 'Team deleted successfully'
//         });
//     } catch (error) {
//         console.error('Error deleting team:', error);
//         res.status(500).json({
//             success: false,
//             message: 'Failed to delete team'
//         });
//     }
// });

// /**
//  * POST /api/teams/:id/assign-user
//  * Assign user to team (and optionally set parent user)
//  * Body: { user_id, parent_user_id (optional) }
//  */
// router.post('/:id/assign-user', authenticateToken, requireRole(['super_admin', 'admin']), async (req, res) => {
//     try {
//         const teamId = Number(req.params.id);
//         const { user_id, parent_user_id } = req.body;

//         if (!user_id) {
//             return res.status(400).json({
//                 success: false,
//                 message: 'user_id is required'
//             });
//         }

//         const [teamResult, userResult] = await Promise.all([
//             pool.query(
//                 'SELECT id FROM team_master WHERE id = ? AND is_active = 1',
//                 [teamId]
//             ),
//             pool.query(
//                 'SELECT id FROM users WHERE id = ? AND isActive = 1',
//                 [user_id]
//             )
//         ]);

//         if (!teamResult.recordset.length) {
//             return res.status(404).json({
//                 success: false,
//                 message: 'Team not found or inactive'
//             });
//         }

//         if (!userResult.recordset.length) {
//             return res.status(404).json({
//                 success: false,
//                 message: 'User not found or inactive'
//             });
//         }

//         if (parent_user_id) {
//             if (Number(parent_user_id) === Number(user_id)) {
//                 return res.status(400).json({
//                     success: false,
//                     message: 'User cannot be their own parent'
//                 });
//             }

//             const parentResult = await pool.query(
//                 'SELECT id FROM users WHERE id = ? AND isActive = 1',
//                 [parent_user_id]
//             );

//             if (!parentResult.recordset.length) {
//                 return res.status(404).json({
//                     success: false,
//                     message: 'Parent user not found or inactive'
//                 });
//             }
//         }

//         const fields = ['team_id = ?'];
//         const values = [teamId];

//         if (parent_user_id !== undefined) {
//             fields.push('parent_id = ?');
//             values.push(parent_user_id || null);
//         }

//         values.push(user_id);

//         await pool.query(
//             `UPDATE users SET ${fields.join(', ')} WHERE id = ?`,
//             values
//         );

//         const updatedUser = await pool.query(
//             `
//       SELECT
//         u.id,
//         u.username,
//         u.email,
//         u.role,
//         u.team_id,
//         u.parent_id,
//         tm.name AS team_name,
//         tm.team_code,
//         p.username AS parent_username,
//         p.email AS parent_email
//       FROM users u
//       LEFT JOIN team_master tm ON tm.id = u.team_id
//       LEFT JOIN users p ON p.id = u.parent_id
//       WHERE u.id = ?
//       `,
//             [user_id]
//         );

//         res.status(200).json({
//             success: true,
//             message: 'User assigned to team successfully',
//             data: updatedUser.recordset[0]
//         });
//     } catch {
//         res.status(500).json({
//             success: false,
//             message: 'Failed to assign user to team'
//         });
//     }
// });

// // /**
// //  * GET /api/teams/:id/users
// //  * Get all users in a team
// //  */

// router.get('/:id/users', authenticateToken, requireRole(['super_admin', 'admin', 'leader']), async (req, res) => {
//     try {
//         const teamId = Number(req.params.id);

//         const result = await pool.query(
//             `
//       SELECT
//         u.id,
//         u.username,
//         u.email,
//         u.mobile_no AS mobile,
//         u.role,
//         u.parent_id,
//         u.isActive,
//         u.createdAt,
//         p.username AS parent_username,
//         p.email AS parent_email
//       FROM users u
//       LEFT JOIN users p ON p.id = u.parent_id
//       WHERE u.team_id = ? AND u.isActive = 1
//       ORDER BY u.username
//       `,
//             [teamId]
//         );

//         res.status(200).json({
//             success: true,
//             data: result.recordset
//         });
//     } catch {
//         res.status(500).json({
//             success: false,
//             message: 'Failed to fetch team users'
//         });
//     }
// });

// // /**
// //  * GET /api/teams/:id/hierarchy
// //  * Get team hierarchy with parent-child user relationships organized in tree structure
// //  */
// router.get('/:id/hierarchy', authenticateToken, requireRole(['super_admin', 'admin', 'leader']), async (req, res) => {
//     try {
//         const teamId = Number(req.params.id);

//         const teamResult = await pool.query(
//             `
//       SELECT
//         tm.*,
//         p.name AS parent_name,
//         p.team_code AS parent_team_code,
//         u.username AS created_by_username,
//         (SELECT COUNT(*) FROM users WHERE team_id = tm.id AND isActive = 1) AS user_count,
//         (SELECT COUNT(*) FROM team_master WHERE parent_id = tm.id AND is_active = 1) AS child_team_count
//       FROM team_master tm
//       LEFT JOIN team_master p ON p.id = tm.parent_id
//       LEFT JOIN users u ON u.id = tm.created_by
//       WHERE tm.id = ?
//       `,
//             [teamId]
//         );

//         if (!teamResult.recordset.length) {
//             return res.status(404).json({ success: false, message: 'Team not found' });
//         }

//         const team = teamResult.recordset[0];

//         const usersResult = await pool.query(
//             `
//       SELECT
//         u.id,
//         u.username,
//         u.email,
//         u.mobile_no AS mobile,
//         u.role,
//         u.parent_id,
//         u.isActive,
//         u.createdAt,
//         p.username AS parent_username,
//         p.email AS parent_email,
//         p.role AS parent_role,
//         (SELECT COUNT(*) FROM users WHERE parent_id = u.id AND isActive = 1) AS child_count
//       FROM users u
//       LEFT JOIN users p ON p.id = u.parent_id
//       WHERE u.team_id = ? AND u.isActive = 1
//       ORDER BY u.parent_id IS NULL DESC, u.username
//       `,
//             [teamId]
//         );

//         const users = usersResult.recordset;
//         const userMap = new Map();
//         const roots = [];

//         for (const u of users) {
//             u.children = [];
//             userMap.set(u.id, u);
//         }

//         for (const u of users) {
//             if (u.parent_id && userMap.has(u.parent_id)) {
//                 userMap.get(u.parent_id).children.push(u);
//             } else {
//                 roots.push(u);
//             }
//         }

//         const childTeamsResult = await pool.query(
//             `
//       SELECT
//         id,
//         name,
//         description,
//         team_code,
//         hierarchy_level,
//         (SELECT COUNT(*) FROM users WHERE team_id = team_master.id AND isActive = 1) AS user_count
//       FROM team_master
//       WHERE parent_id = ? AND is_active = 1
//       ORDER BY name
//       `,
//             [teamId]
//         );

//         res.status(200).json({
//             success: true,
//             data: {
//                 team: {
//                     id: team.id,
//                     name: team.name,
//                     description: team.description,
//                     team_code: team.team_code,
//                     parent_id: team.parent_id,
//                     parent_name: team.parent_name,
//                     parent_team_code: team.parent_team_code,
//                     hierarchy_level: team.hierarchy_level,
//                     user_count: team.user_count || 0,
//                     child_team_count: team.child_team_count || 0,
//                     created_by_username: team.created_by_username,
//                     created_at: team.created_at
//                 },
//                 user_hierarchy: roots,
//                 flat_users: users,
//                 child_teams: childTeamsResult.recordset
//             }
//         });
//     } catch {
//         res.status(500).json({ success: false, message: 'Failed to fetch team hierarchy' });
//     }
// });

router.post('/users/parents',[authenticateToken,requirePermission('users:update')], teamController.addParentToUser);
router.get('/users/:id/members',[authenticateToken],teamController.getUserMembers);
router.delete("/users/:id/parents/:parentId",[authenticateToken,requirePermission("users:update")],teamController.deleteUserMemeber);

module.exports = router