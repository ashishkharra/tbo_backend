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


module.exports = {
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
  removeParentRelationship
};
