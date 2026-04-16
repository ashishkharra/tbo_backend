const { pool } = require("../config/config")
const { safeParse } = require('../helper/helper.js')

async function isTeamCodeExists(teamCode) {
  try {
    const { rows } = await pool.query(
      "SELECT id FROM team_master WHERE team_code = $1",
      [teamCode]
    );
    return rows.length > 0;
  } catch (error) {
    console.error("isTeamCodeExists error:", error);
    throw error;
  }
}

async function calculateHierarchyPath(parentId) {
  try {
    if (!parentId) {
      return { level: 1, path: "1" };
    }

    const { rows } = await pool.query(
      "SELECT hierarchy_level, hierarchy_path FROM team_master WHERE id = $1",
      [parentId]
    );

    if (rows.length === 0) {
      throw new Error("Invalid parent team");
    }

    const parent = rows[0];
    return {
      level: parent.hierarchy_level + 1,
      path: `${parent.hierarchy_path}.${parent.hierarchy_level + 1}`,
    };
  } catch (error) {
    console.error("calculateHierarchyPath error:", error);
    throw error;
  }
}

async function insertTeam(data) {
  try {
    const { rows } = await pool.query(
      `
      INSERT INTO team_master
      (name, description, parent_id, team_code, hierarchy_level, hierarchy_path, is_active, created_by)
      VALUES ($1,$2,$3,$4,$5,$6,true,$7)
      RETURNING id
      `,
      [
        data.name,
        data.description,
        data.parent_id,
        data.team_code,
        data.level,
        data.path,
        data.createdBy,
      ]
    );

    return rows[0].id;
  } catch (error) {
    console.error("insertTeam error:", error);
    throw error;
  }
}

async function assignCreatorToTeam(teamId, userId) {
  try {
    await pool.query(
      `
      UPDATE users
      SET team_id = $1, parent_id = NULL
      WHERE id = $2 AND is_active = true
      `,
      [teamId, userId]
    );
    return true;
  } catch (error) {
    console.error("assignCreatorToTeam error:", error);
    throw error;
  }
}

async function getTeamById(teamId) {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM team_master WHERE id = $1",
      [teamId]
    );
    return rows[0] || null;
  } catch (error) {
    console.error("getTeamById error:", error);
    throw error;
  }
}

async function getActiveUserById(userId) {
  const { rows } = await pool.query(
    `SELECT id, username, permissions
     FROM users
     WHERE id = $1 AND "is_active" = true`,
    [userId]
  );
  return rows[0];
}

async function checkExistingParentRelation(userId, parentId) {
  const { rows } = await pool.query(
    `SELECT 1
     FROM user_parents
     WHERE user_id = $1
       AND parent_id = $2
       AND is_active = true`,
    [userId, parentId]
  );
  return rows.length > 0;
}

async function insertParentRelation(userId, parentId, assignedBy) {
  await pool.query(
    `INSERT INTO user_parents (user_id, parent_id, assigned_by, is_active)
     VALUES ($1, $2, $3, true)`,
    [userId, parentId, assignedBy]
  );
}

async function updateUserPermissions(userId, permissionIds) {
  await pool.query(
    `UPDATE users SET permissions = $1 WHERE id = $2`,
    [JSON.stringify(permissionIds), userId]
  );
}

async function rebuildUserPermissions(client, userId, permissionKeys = []) {
  await client.query(
    `DELETE FROM user_permissions WHERE user_id = $1`,
    [userId]
  );

  if (!permissionKeys.length) return [];

  const values = permissionKeys
    .map((_, i) => `($1, $${i + 2})`)
    .join(',');

  const result = await client.query(
    `
    INSERT INTO user_permissions (user_id, permission_key)
    VALUES ${values}
    RETURNING id
    `,
    [userId, ...permissionKeys]
  );

  return result.rows.map(r => r.id);
}


async function syncPermissionsToParent(childId, parentId) {
  const childRes = await pool.query(
    `SELECT permissions FROM users WHERE id = $1`,
    [childId]
  );

  const parentRes = await pool.query(
    `SELECT permissions FROM users WHERE id = $1`,
    [parentId]
  );

  if (!childRes.rows.length || !parentRes.rows.length) return;

  const childPerms = safeParse(childRes.rows[0].permissions);
  const parentPerms = safeParse(parentRes.rows[0].permissions);

  const merged = [...new Set([...parentPerms, ...childPerms])];

  await pool.query(
    `UPDATE users SET permissions = $1 WHERE id = $2`,
    [JSON.stringify(merged), parentId]
  );
}

async function removeParentRelationship(userId, parentId) {
  const query = `
    UPDATE user_parents
    SET is_active = FALSE
    WHERE user_id = $1 AND parent_id = $2 AND is_active = TRUE
  `;
  await pool.query(query, [userId, parentId]);
}

async function syncPermissionsFromParent(childId, parentId) {
  try {
    const childrenResult = await pool.query(`
      SELECT u.id, u.permissions
      FROM user_parents up
      INNER JOIN users u ON up.user_id = u.id
      WHERE up.parent_id = $1 AND up.is_active = TRUE AND u.id != $2 AND u."is_active" = TRUE
    `, [parentId, childId]);

    const allChildrenPermissions = new Set();
    childrenResult.rows.forEach(child => {
      const childPerms = JSON.parse(child.permissions || '[]');
      if (Array.isArray(childPerms)) childPerms.forEach(perm => allChildrenPermissions.add(perm));
    });

    const removedChildResult = await pool.query('SELECT permissions FROM users WHERE id = $1', [childId]);
    if (!removedChildResult.rows.length) return;

    const removedChildPermissions = JSON.parse(removedChildResult.rows[0].permissions || '[]');
    const removedChildPermissionsArray = Array.isArray(removedChildPermissions) ? removedChildPermissions : [];

    const parentResult = await pool.query('SELECT permissions FROM users WHERE id = $1', [parentId]);
    if (!parentResult.rows.length) return;

    const parentPermissions = JSON.parse(parentResult.rows[0].permissions || '[]');
    const parentPermissionsArray = Array.isArray(parentPermissions) ? parentPermissions : [];

    const updatedPermissions = parentPermissionsArray.filter(perm =>
      allChildrenPermissions.has(perm) || !removedChildPermissionsArray.includes(perm)
    );

    await pool.query('UPDATE users SET permissions = $1 WHERE id = $2', [JSON.stringify(updatedPermissions), parentId]);
  } catch (error) {
    console.error('Error syncing permissions from parent:', error);
  }
}

async function getMembersByParentId(parentId, limit, offset) {
  const { rows } = await pool.query(
    `
  SELECT
    up.id                AS relationship_id,
    up.assigned_at,
    up.assigned_by,

    u.id                 AS user_id,
    u.username,
    u.email,
    u.mobile_no          AS mobile,
    u.role,
    u.assigned_modules,
    u.is_active,

    COALESCE(
      json_agg(
        json_build_object(
          'id', cu.id,
          'username', cu.username
        )
      ) FILTER (WHERE cu.id IS NOT NULL),
      '[]'
    ) AS sub_members

  FROM user_parents up
  JOIN users u 
    ON u.id = up.user_id

  LEFT JOIN user_parents up2
    ON up2.parent_id = u.id
    AND up2.is_active = true

  LEFT JOIN users cu
    ON cu.id = up2.user_id
    AND cu.is_active = true

  WHERE up.parent_id = $1
    AND up.is_active = true
    AND u.is_active = true

  GROUP BY up.id, u.id
  ORDER BY up.assigned_at DESC
  LIMIT $2 OFFSET $3
  `,
    [parentId, limit, offset]
  );
  return rows;
}

async function countMembersByParentId(parentId) {
  const { rows } = await pool.query(
    `
    SELECT COUNT(*)::int AS total
    FROM user_parents up
    INNER JOIN users u ON u.id = up.user_id
    WHERE up.parent_id = $1
      AND up.is_active = true
      AND u."is_active" = true
    `,
    [parentId]
  );

  return rows[0].total;
}

async function removeParentRelationship(userId, parentId) {
  const query = `
    UPDATE user_parents
    SET is_active = FALSE
    WHERE user_id = $1 AND parent_id = $2 AND is_active = TRUE
  `;
  await pool.query(query, [userId, parentId]);
}

// new team apis ->>>>>>>>>>>>>>>>>>>>>>>>>>>>>

const getUsersRoles = async (currentUser, filters = {}) => {
  const {
    search = "",
    role = "",
    limit = 50,
  } = filters;

  const roleParams = [];
  const userParams = [];

  let roleParamIndex = 1;
  let userParamIndex = 1;

  let roleWhere = ` WHERE 1=1 `;
  let userWhere = ` WHERE 1=1 `;

  // search filter
  if (search && String(search).trim()) {
    roleWhere += `
      AND (
        r.name ~* $${roleParamIndex}
        OR COALESCE(r.code, '') ~* $${roleParamIndex}
        OR CAST(r.id AS TEXT) ~* $${roleParamIndex}
      )
    `;
    roleParams.push(String(search).trim());
    roleParamIndex++;

    userWhere += `
      AND (
        COALESCE(u.username, '') ~* $${userParamIndex}
        OR COALESCE(u.role, '') ~* $${userParamIndex}
        OR CAST(u.id AS TEXT) ~* $${userParamIndex}
      )
    `;
    userParams.push(String(search).trim());
    userParamIndex++;
  }

  // selected role filter for users only
  if (role && String(role).trim() && String(role).trim() !== "all") {
    userWhere += `
      AND (
        LOWER(COALESCE(u.role, '')) = LOWER($${userParamIndex})
        OR EXISTS (
          SELECT 1
          FROM roles rr
          WHERE rr.id::text = $${userParamIndex}
            AND LOWER(COALESCE(rr.code, '')) = LOWER(COALESCE(u.role, ''))
        )
        OR EXISTS (
          SELECT 1
          FROM roles rr
          WHERE rr.id::text = $${userParamIndex}
            AND LOWER(COALESCE(rr.name, '')) = LOWER(COALESCE(u.role, ''))
        )
      )
    `;
    userParams.push(String(role).trim());
    userParamIndex++;
  }

  // role-based visibility for users
  const currentRole = String(currentUser?.role || "").toLowerCase();
  const currentUserId = Number(currentUser?.id || 0);

  if (currentRole === "super_admin") {
    // no restriction
  } else if (currentRole === "admin") {
    userWhere += `
      AND LOWER(COALESCE(u.role, '')) != 'super_admin'
    `;

    if (currentUserId) {
      userWhere += ` AND u.id != $${userParamIndex} `;
      userParams.push(currentUserId);
      userParamIndex++;
    }
  } else {
    userWhere += `
      AND LOWER(COALESCE(u.role, '')) NOT IN ('super_admin', 'admin')
    `;

    if (currentUserId) {
      userWhere += ` AND u.id != $${userParamIndex} `;
      userParams.push(currentUserId);
      userParamIndex++;
    }
  }

  roleParams.push(Number(limit) || 50);
  const roleLimitParam = roleParamIndex;

  userParams.push(Number(limit) || 50);
  const userLimitParam = userParamIndex;

  const rolesQuery = `
    SELECT
      r.id,
      r.name,
      r.code
    FROM roles r
    ${roleWhere}
    ORDER BY r.name ASC
    LIMIT $${roleLimitParam}
  `;

  const usersQuery = `
    SELECT
      u.id,
      u.username,
      u.role
    FROM users u
    ${userWhere}
    ORDER BY u.username ASC
    LIMIT $${userLimitParam}
  `;

  const [rolesRes, usersRes] = await Promise.all([
    pool.query(rolesQuery, roleParams),
    pool.query(usersQuery, userParams),
  ]);

  return {
    roles: rolesRes.rows,
    users: usersRes.rows,
  };
};

/**
 * Add parent-child link
 */
async function addParentChildLink({ parent_user_id, child_user_id, role = null }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const parentId = Number(parent_user_id);
    const childId = Number(child_user_id);

    if (!parentId || !childId) {
      throw new Error("Valid parent_user_id and child_user_id are required");
    }

    if (parentId === childId) {
      throw new Error("A user cannot be parent of itself");
    }

    const userCheck = await client.query(
      `
      SELECT id, role
      FROM users
      WHERE id = ANY($1::bigint[])
      `,
      [[parentId, childId]]
    );

    if (userCheck.rowCount !== 2) {
      throw new Error("Parent or child user not found");
    }

    const parentUser = userCheck.rows.find(
      (row) => Number(row.id) === parentId
    );
    const childUser = userCheck.rows.find(
      (row) => Number(row.id) === childId
    );

    const parentRole = String(parentUser?.role || "").trim().toLowerCase();
    const childRole = String(childUser?.role || "").trim().toLowerCase();

    // Rule 1: super_admin cannot be under anyone
    if (childRole === "super_admin") {
      throw new Error("super_admin cannot be assigned under any role");
    }

    // Rule 2: admin can only be under super_admin
    if (childRole === "admin" && parentRole !== "super_admin") {
      throw new Error("admin can only be assigned under super_admin");
    }

    const result = await client.query(
      `
      INSERT INTO user_hierarchy (
        parent_user_id,
        child_user_id,
        role
      )
      VALUES ($1, $2, $3)
      ON CONFLICT (parent_user_id, child_user_id)
      DO UPDATE SET
        role = EXCLUDED.role,
        updated_at = NOW()
      RETURNING *
      `,
      [parentId, childId, role]
    );

    await client.query("COMMIT");

    return {
      success: true,
      message: "Parent-child link saved successfully",
      data: result.rows[0]
    };
  } catch (error) {
    await client.query("ROLLBACK");
    return {
      success: false,
      message: error.message || "Failed to save parent-child link"
    };
  } finally {
    client.release();
  }
}

/**
 * Remove parent-child link
 */
async function removeParentChildLink({ parent_user_id, child_user_id }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const parentId = Number(parent_user_id);
    const childId = Number(child_user_id);

    if (!parentId || !childId) {
      throw new Error("Valid parent_user_id and child_user_id are required");
    }

    const result = await client.query(
      `
      DELETE FROM user_hierarchy
      WHERE parent_user_id = $1
        AND child_user_id = $2
      RETURNING *
      `,
      [parentId, childId]
    );

    await client.query("COMMIT");

    if (result.rowCount === 0) {
      return {
        success: false,
        message: "Parent-child link not found"
      };
    }

    return {
      success: true,
      message: "Parent-child link removed successfully",
      data: result.rows[0]
    };
  } catch (error) {
    await client.query("ROLLBACK");
    return {
      success: false,
      message: error.message || "Failed to remove parent-child link"
    };
  } finally {
    client.release();
  }
}

/**
 * Get direct children
 */
async function getDirectChildren(user_id) {
  try {
    const result = await pool.query(
      `
      SELECT
        uh.id,
        uh.parent_user_id,
        uh.child_user_id,
        uh.role AS hierarchy_role,
        u.name,
        u.username,
        u.mobile_no,
        u.login_access,
        u.role
      FROM user_hierarchy uh
      INNER JOIN users u
        ON u.id = uh.child_user_id
      WHERE uh.parent_user_id = $1
      ORDER BY u.name NULLS LAST, u.username NULLS LAST
      `,
      [user_id]
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to fetch children"
    };
  }
}

/**
 * Get direct parents
 */
async function getDirectParents(user_id) {
  try {
    const result = await pool.query(
      `
      SELECT
        uh.id,
        uh.parent_user_id,
        uh.child_user_id,
        uh.role AS hierarchy_role,
        u.name,
        u.username,
        u.mobile_no,
        u.login_access,
        u.role
      FROM user_hierarchy uh
      INNER JOIN users u
        ON u.id = uh.parent_user_id
      WHERE uh.child_user_id = $1
      ORDER BY u.name NULLS LAST, u.username NULLS LAST
      `,
      [user_id]
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to fetch parents"
    };
  }
}

/**
 * Get all descendants
 */
async function getAllChildren(user_id) {
  try {
    const result = await pool.query(
      `
      SELECT
        c.child_user_id,
        c.level_no,
        u.name,
        u.username,
        u.mobile_no,
        u.login_access,
        u.role
      FROM get_all_children($1) c
      INNER JOIN users u
        ON u.id = c.child_user_id
      ORDER BY c.level_no, u.name NULLS LAST, u.username NULLS LAST
      `,
      [user_id]
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to fetch all children"
    };
  }
}

/**
 * Get all ancestors
 */
async function getAllParents(user_id) {
  try {
    const result = await pool.query(
      `
      SELECT
        p.parent_user_id,
        p.level_no,
        u.name,
        u.username,
        u.mobile_no,
        u.login_access,
        u.role
      FROM get_all_parents($1) p
      INNER JOIN users u
        ON u.id = p.parent_user_id
      ORDER BY p.level_no, u.name NULLS LAST, u.username NULLS LAST
      `,
      [user_id]
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to fetch all parents"
    };
  }
}

/**
 * Effective permissions as module-action pairs
 */
async function getEffectivePermissions(user_id) {
  try {
    const result = await pool.query(
      `
      SELECT
        ep.module_id,
        pm.name AS module_name,
        pm.code AS module_code,
        ep.action_id,
        pa.name AS action_name,
        pa.code AS action_code
      FROM get_effective_user_permissions($1) ep
      INNER JOIN permission_modules pm
        ON pm.id = ep.module_id
      INNER JOIN permission_actions pa
        ON pa.id = ep.action_id
      ORDER BY ep.module_id, ep.action_id
      `,
      [user_id]
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to resolve effective permissions"
    };
  }
}

/**
 * Effective permissions grouped by module
 */
async function getEffectivePermissionsGrouped(user_id) {
  try {
    const result = await pool.query(
      `
      WITH grouped AS (
        SELECT *
        FROM get_effective_user_permissions_grouped($1)
      )
      SELECT
        g.module_id,
        pm.name AS module_name,
        pm.code AS module_code,
        g.actions
      FROM grouped g
      INNER JOIN permission_modules pm
        ON pm.id = g.module_id
      ORDER BY g.module_id
      `,
      [user_id]
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to resolve grouped effective permissions"
    };
  }
}

/**
 * Refresh team table manually
 */
async function refreshTeams() {
  try {
    await pool.query(`SELECT refresh_teams()`);
    return {
      success: true,
      message: "Teams refreshed successfully"
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to refresh teams"
    };
  }
}

/**
 * Get teams rows
 */
async function getTeams(filters = {}) {
  try {
    const {
      parent_user_id = null,
      child_user_id = null,
      module_id = null
    } = filters;

    const params = [];
    let where = ` WHERE 1=1 `;
    let i = 1;

    if (parent_user_id) {
      where += ` AND t.parent_user_id = $${i} `;
      params.push(parent_user_id);
      i++;
    }

    if (child_user_id) {
      where += ` AND t.child_user_id = $${i} `;
      params.push(child_user_id);
      i++;
    }

    if (module_id) {
      where += ` AND t.modules @> $${i}::jsonb `;
      params.push(JSON.stringify([Number(module_id)]));
      i++;
    }

    const result = await pool.query(
      `
      SELECT
        t.*
      FROM teams t
      ${where}
      ORDER BY t.parent_user_id, t.child_user_id
      `,
      params
    );

    return {
      success: true,
      data: result.rows
    };
  } catch (error) {
    return {
      success: false,
      message: error.message || "Failed to fetch teams"
    };
  }
}



module.exports = {
  getUsersRoles,
  isTeamCodeExists,
  calculateHierarchyPath,
  insertTeam,
  assignCreatorToTeam,
  getTeamById,
  getActiveUserById,
  checkExistingParentRelation,
  insertParentRelation,
  updateUserPermissions,
  rebuildUserPermissions,
  syncPermissionsToParent,
  syncPermissionsFromParent,
  getMembersByParentId,
  countMembersByParentId,
  removeParentRelationship,
  addParentChildLink,
  removeParentChildLink,
  getDirectChildren,
  getDirectParents,
  getAllChildren,
  getAllParents,
  getEffectivePermissions,
  getEffectivePermissionsGrouped,
  refreshTeams,
  getTeams
};
