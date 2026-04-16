const { pool } = require("../config/config");
const crypto = require("crypto");


const generateModulesCode = async (client) => {
  let code;
  let exists = true;

  while (exists) {
    const random = crypto.randomBytes(4).toString("hex").toUpperCase();
    code = `MOD-${random}`;

    const check = await client.query(
      `SELECT id FROM users WHERE modules_code = $1 LIMIT 1`,
      [code]
    );

    exists = check.rows.length > 0;
  }

  return code;
};


const getModuleHierarchy = async (client, moduleCode) => {
  const result = await client.query(
    `
    WITH RECURSIVE module_tree AS (
      SELECT id, name, code, parent_id, level_no
      FROM permission_modules
      WHERE code = $1

      UNION ALL

      SELECT pm.id, pm.name, pm.code, pm.parent_id, pm.level_no
      FROM permission_modules pm
      INNER JOIN module_tree mt ON mt.parent_id = pm.id
    )
    SELECT *
    FROM module_tree
    ORDER BY level_no;
    `,
    [moduleCode]
  );

  return result.rows;
};




// main
async function getAdminData(contact_no) {
  const sql = 'SELECT * FROM users WHERE mobile_no = $1';
  try {
    const result = await pool.query(sql, [contact_no]);
    return result.rows;
  } catch (err) {
    throw err;
  }
}

async function getProfileModel(token) {
  try {
    const result = await pool.query(
      `
      SELECT
        id,
        username,
        email,
        mobile_no,
        role,
        last_login,
        email_verified,
        is_active,
        created_at,
        updated_at
      FROM users
      WHERE token = $1 AND is_active = true
      `,
      [token]
    );
    return result;
  } catch (err) {
    throw err;
  }
}

async function insertLoginHistory(user_id, ip_address, user_agent) {
  const sql = `
        INSERT INTO tbl_login_history (user_id, ip_address, user_agent)
        VALUES (?, ?, ?)
    `;

  try {
    await pool.promise().execute(sql, [user_id, ip_address, user_agent]);
    return true;
  } catch (err) {
    throw err;
  }
};

async function fetchUsers(currentUser, filters = {}) {
  const {
    role = "all",
    parent_id = "all",
    username = "all",
    status = "all",
    search = "",
    page = 1,
    limit = 1000,
  } = filters;

  const offset = (page - 1) * limit;

  let whereConditions = `
    WHERE u.id != $1
  `;

  const params = [currentUser.id];
  let paramIndex = 2;

  // role based visibility
  if (currentUser.role === "leader") {
    whereConditions += `
      AND u.role = 'data_entry_operator'
      AND u.created_by = $${paramIndex}
    `;
    params.push(currentUser.id);
    paramIndex++;
  }

  // active/inactive/all
  if (status === "active") {
    whereConditions += ` AND u.is_active = true `;
  } else if (status === "inactive") {
    whereConditions += ` AND u.is_active = false `;
  }

  // role filter
  if (role && role !== "all") {
    whereConditions += ` AND u.role = $${paramIndex} `;
    params.push(role);
    paramIndex++;
  }

  // parent filter
  if (parent_id && parent_id !== "all") {
    whereConditions += ` AND u.parent_id = $${paramIndex} `;
    params.push(Number(parent_id));
    paramIndex++;
  }

  // username filter
  if (username && username !== "all") {
    whereConditions += ` AND u.username = $${paramIndex} `;
    params.push(username);
    paramIndex++;
  }

  // search filter
  if (search && search.trim()) {
    whereConditions += `
      AND (
        u.username ILIKE $${paramIndex}
        OR u.email ILIKE $${paramIndex}
        OR u.mobile_no ILIKE $${paramIndex}
        OR u.authenticated_email ILIKE $${paramIndex}
        OR u.role ILIKE $${paramIndex}
      )
    `;
    params.push(`%${search.trim()}%`);
    paramIndex++;
  }

  const baseFrom = `
    FROM users u
    LEFT JOIN LATERAL (
      SELECT STRING_AGG(assignment_token, ',') AS assignment_tokens
      FROM user_assignments
      WHERE user_id = u.id
    ) ua ON true
    ${whereConditions}
  `;

  const dataQuery = `
    SELECT
      u.id,
      u.token,
      u.username,
      u.email,
      u.mobile_no AS mobile,
      u.authenticated_email,
      u.role,
      u.permissions,
      u.is_active,
      u.created_at,
      u.last_login,
      u.created_by,
      u.address,
      u.location,
      u.team_id,
      u.parent_id,

      u.assigned_modules             AS assignedmodules,
      u.assigned_sub_modules         AS assignedsubmodules,
      u.assigned_datasets            AS assigneddatasets,
      u.dataset_access               AS datasetaccess,
      u.data_assignment              AS dataassignment,
      u.hierarchical_data_assignment AS hierarchicaldataassignment,

      u.module_permissions,
      u.modules_code,
      u.permission_code,
      u.assign_code,

      ua.assignment_tokens
    ${baseFrom}
    ORDER BY id ASC
    LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
  `;

  const countQuery = `
    SELECT COUNT(*)::int AS total
    ${baseFrom}
  `;

  const dataParams = [...params, limit, offset];
  const countParams = [...params];

  try {
    const [dataResult, countResult] = await Promise.all([
      pool.query(dataQuery, dataParams),
      pool.query(countQuery, countParams),
    ]);

    return {
      rows: dataResult.rows,
      total: countResult.rows[0]?.total || 0,
    };
  } catch (error) {
    throw error;
  }
}

async function bulkUpdateUsers (users, currentUser) {
  const client = await pool.connect();

  console.log("🚀 BULK UPDATE START");
  console.log("👉 users => ", JSON.stringify(users, null, 2));
  console.log("👉 currentUser => ", currentUser);

  const copyUserPermissions = async (targetUserId, sourceUserId) => {
    console.log("🔁 copyUserPermissions START", { targetUserId, sourceUserId });

    await client.query(
      `DELETE FROM user_permissions WHERE user_id = $1`,
      [targetUserId]
    );
    console.log("🗑️ Deleted old user_permissions for target user:", targetUserId);

    const insertPermissionsRes = await client.query(
      `
      INSERT INTO user_permissions (
        user_id,
        module_id,
        action_id,
        is_allowed,
        created_at,
        updated_at
      )
      SELECT
        $1,
        up.module_id,
        up.action_id,
        up.is_allowed,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
      FROM user_permissions up
      WHERE up.user_id = $2
      RETURNING id, module_id, action_id, is_allowed
      `,
      [targetUserId, sourceUserId]
    );

    console.log(
      "✅ Copied user_permissions count =>",
      insertPermissionsRes.rows.length
    );
    console.log("✅ Copied user_permissions rows =>", insertPermissionsRes.rows);

    return insertPermissionsRes.rows.length;
  };

  const copyAssignmentsAndColumnPermissions = async (targetUserId, sourceUserId) => {
    console.log("🔁 copyAssignmentsAndColumnPermissions START", {
      targetUserId,
      sourceUserId,
    });

    await client.query(
      `DELETE FROM user_column_permissions WHERE user_id = $1`,
      [targetUserId]
    );
    console.log("🗑️ Deleted old user_column_permissions for:", targetUserId);

    await client.query(
      `DELETE FROM user_data_assignments WHERE user_id = $1`,
      [targetUserId]
    );
    console.log("🗑️ Deleted old user_data_assignments for:", targetUserId);

    const sourceAssignmentsRes = await client.query(
      `
      SELECT *
      FROM user_data_assignments
      WHERE user_id = $1
      ORDER BY id ASC
      `,
      [sourceUserId]
    );

    console.log(
      "📚 sourceAssignments count =>",
      sourceAssignmentsRes.rows.length
    );
    console.log("📚 sourceAssignments rows =>", sourceAssignmentsRes.rows);

    const assignmentIdMap = {};

    for (const item of sourceAssignmentsRes.rows) {
      console.log("➕ Copying assignment =>", item);

      const insertAssignmentRes = await client.query(
        `
        INSERT INTO user_data_assignments (
          user_id,
          db_table,
          wise_type,
          district,
          ac,
          pc,
          party_jila,
          data_id,
          block_id,
          gp_ward_id,
          village_id,
          ac_id,
          bhag_no,
          sec_no,
          mandal_id,
          kendra_id,
          created_by,
          updated_by,
          created_at,
          updated_at,
          is_active,
          age_from,
          age_to,
          cast_filter
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
          $17, $18, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $19, $20, $21, $22
        )
        RETURNING id
        `,
        [
          targetUserId,
          item.db_table,
          item.wise_type,
          item.district,
          item.ac,
          item.pc,
          item.party_jila,
          item.data_id,
          item.block_id,
          item.gp_ward_id,
          item.village_id,
          item.ac_id,
          item.bhag_no,
          item.sec_no,
          item.mandal_id,
          item.kendra_id,
          currentUser?.id || null,
          currentUser?.id || null,
          item.is_active ?? 1,
          item.age_from,
          item.age_to,
          item.cast_filter,
        ]
      );

      const newAssignmentId = insertAssignmentRes.rows[0].id;
      assignmentIdMap[item.id] = newAssignmentId;

      console.log(`✅ Assignment copied old=${item.id}, new=${newAssignmentId}`);
    }

    const sourceColumnPermissionsRes = await client.query(
      `
      SELECT *
      FROM user_column_permissions
      WHERE user_id = $1
      ORDER BY id ASC
      `,
      [sourceUserId]
    );

    console.log(
      "📚 sourceColumnPermissions count =>",
      sourceColumnPermissionsRes.rows.length
    );
    console.log(
      "📚 sourceColumnPermissions rows =>",
      sourceColumnPermissionsRes.rows
    );

    for (const col of sourceColumnPermissionsRes.rows) {
      const newAssignmentId = col.assignment_id
        ? assignmentIdMap[col.assignment_id] || null
        : null;

      console.log("➕ Copying column permission =>", {
        old_assignment_id: col.assignment_id,
        new_assignment_id: newAssignmentId,
        column_name: col.column_name,
        db_table: col.db_table,
      });

      await client.query(
        `
        INSERT INTO user_column_permissions (
          user_id,
          assignment_id,
          db_table,
          column_name,
          can_view,
          can_mask,
          can_edit,
          can_copy,
          created_by,
          updated_by,
          created_at,
          updated_at
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        `,
        [
          targetUserId,
          newAssignmentId,
          col.db_table,
          col.column_name,
          col.can_view,
          col.can_mask,
          col.can_edit,
          col.can_copy,
          currentUser?.id || null,
          currentUser?.id || null,
        ]
      );
    }

    console.log("✅ copyAssignmentsAndColumnPermissions DONE", {
      targetUserId,
      sourceUserId,
      assignmentMap: assignmentIdMap,
    });

    return true;
  };

  try {
    await client.query("BEGIN");
    console.log("✅ TRANSACTION STARTED");

    const updatedRows = [];

    for (const user of users) {
      console.log("\n==============================");
      console.log("🔄 Processing user =>", user);

      if (!user.id) {
        console.log("⛔ Skipped: user.id missing");
        continue;
      }

      const targetUserId = Number(user.id);
      if (!targetUserId) {
        console.log("⛔ Invalid target user id =>", user.id);
        continue;
      }

      console.log("🎯 Target User ID =>", targetUserId);

      const fields = [];
      const values = [];
      let index = 1;

      let moduleSourceUserId = null;
      let permissionSourceUserId = null;

      // -----------------------------
      // Direct/basic fields
      // -----------------------------
      if (user.username !== undefined) {
        fields.push(`username = $${index++}`);
        values.push(user.username);
        console.log("✏️ username =>", user.username);
      }

      if (user.email !== undefined) {
        fields.push(`email = $${index++}`);
        values.push(user.email);
        console.log("✏️ email =>", user.email);
      }

      if (user.mobile !== undefined) {
        fields.push(`mobile_no = $${index++}`);
        values.push(user.mobile);
        console.log("✏️ mobile_no =>", user.mobile);
      }

      if (user.authenticated_email !== undefined) {
        fields.push(`authenticated_email = $${index++}`);
        values.push(user.authenticated_email);
        console.log("✏️ authenticated_email =>", user.authenticated_email);
      }

      if (user.role !== undefined) {
        fields.push(`role = $${index++}`);
        values.push(user.role);
        console.log("✏️ role =>", user.role);
      }

      if (user.is_active !== undefined) {
        fields.push(`is_active = $${index++}`);
        values.push(user.is_active);
        console.log("✏️ is_active =>", user.is_active);
      }

      if (user.parent_id !== undefined) {
        fields.push(`parent_id = $${index++}`);
        values.push(user.parent_id || null);
        console.log("✏️ parent_id =>", user.parent_id || null);
      }

      if (user.team_id !== undefined) {
        fields.push(`team_id = $${index++}`);
        values.push(user.team_id || null);
        console.log("✏️ team_id =>", user.team_id || null);
      }

      if (user.permissions !== undefined) {
        fields.push(`permissions = $${index++}`);
        values.push(
          Array.isArray(user.permissions)
            ? JSON.stringify(user.permissions)
            : user.permissions
        );
        console.log("✏️ permissions direct update");
      }

      if (user.assignedModules !== undefined) {
        fields.push(`assigned_modules = $${index++}`);
        values.push(
          Array.isArray(user.assignedModules)
            ? JSON.stringify(user.assignedModules)
            : user.assignedModules
        );
        console.log("✏️ assigned_modules direct update");
      }

      if (user.assignedSubModules !== undefined) {
        fields.push(`assigned_sub_modules = $${index++}`);
        values.push(
          Array.isArray(user.assignedSubModules)
            ? JSON.stringify(user.assignedSubModules)
            : user.assignedSubModules
        );
        console.log("✏️ assigned_sub_modules direct update");
      }

      if (user.assignedDatasets !== undefined) {
        fields.push(`assigned_datasets = $${index++}`);
        values.push(
          Array.isArray(user.assignedDatasets)
            ? JSON.stringify(user.assignedDatasets)
            : user.assignedDatasets
        );
        console.log("✏️ assigned_datasets direct update");
      }

      // -----------------------------
      // MODULE CODE COPY
      // -----------------------------
      if (user.modules_code !== undefined && String(user.modules_code).trim() !== "") {
        const sourceModuleCode = String(user.modules_code).trim();

        console.log("📦 Module code copy requested from =>", sourceModuleCode);

        const moduleSourceRes = await client.query(
          `
          SELECT
            id,
            modules_code,
            assigned_modules,
            assigned_sub_modules,
            assigned_datasets,
            module_permissions
          FROM users
          WHERE modules_code = $1
            AND id <> $2
          LIMIT 1
          `,
          [sourceModuleCode, targetUserId]
        );

        console.log("🔍 moduleSourceRes.rows =>", moduleSourceRes.rows);

        if (!moduleSourceRes.rows.length) {
          throw new Error(`Invalid modules_code: ${sourceModuleCode}`);
        }

        const sourceUser = moduleSourceRes.rows[0];
        moduleSourceUserId = sourceUser.id;

        console.log("✅ Module source user found =>", sourceUser);

        fields.push(`modules_code = $${index++}`);
        values.push(sourceUser.modules_code || sourceModuleCode);

        fields.push(`assigned_modules = $${index++}`);
        values.push(sourceUser.assigned_modules || "[]");

        fields.push(`assigned_sub_modules = $${index++}`);
        values.push(sourceUser.assigned_sub_modules || "[]");

        fields.push(`assigned_datasets = $${index++}`);
        values.push(sourceUser.assigned_datasets || "[]");

        fields.push(`module_permissions = $${index++}`);
        values.push(sourceUser.module_permissions || null);
      } else if (
        user.modules_code !== undefined &&
        String(user.modules_code).trim() === ""
      ) {
        console.log("🧹 Clearing module code related fields");

        fields.push(`modules_code = $${index++}`);
        values.push(null);

        fields.push(`assigned_modules = $${index++}`);
        values.push("[]");

        fields.push(`assigned_sub_modules = $${index++}`);
        values.push("[]");

        fields.push(`assigned_datasets = $${index++}`);
        values.push("[]");

        fields.push(`module_permissions = $${index++}`);
        values.push(null);
      }

      // -----------------------------
      // PERMISSION CODE COPY
      // -----------------------------
      if (
        user.permission_code !== undefined &&
        String(user.permission_code).trim() !== ""
      ) {
        const sourcePermissionCode = String(user.permission_code).trim();

        console.log("🔐 Permission code copy requested from =>", sourcePermissionCode);

        const permissionSourceRes = await client.query(
          `
          SELECT
            id,
            permission_code,
            permissions
          FROM users
          WHERE permission_code = $1
            AND id <> $2
          LIMIT 1
          `,
          [sourcePermissionCode, targetUserId]
        );

        console.log("🔍 permissionSourceRes.rows =>", permissionSourceRes.rows);

        if (!permissionSourceRes.rows.length) {
          throw new Error(`Invalid permission_code: ${sourcePermissionCode}`);
        }

        const sourceUser = permissionSourceRes.rows[0];
        permissionSourceUserId = sourceUser.id;

        console.log("✅ Permission source user found =>", sourceUser);

        fields.push(`permission_code = $${index++}`);
        values.push(sourceUser.permission_code || sourcePermissionCode);

        fields.push(`permissions = $${index++}`);
        values.push(sourceUser.permissions || "[]");
      } else if (
        user.permission_code !== undefined &&
        String(user.permission_code).trim() === ""
      ) {
        console.log("🧹 Clearing permission code related fields");

        fields.push(`permission_code = $${index++}`);
        values.push(null);

        fields.push(`permissions = $${index++}`);
        values.push("[]");
      }

      if (!fields.length) {
        console.log("⛔ No fields to update for user:", targetUserId);
        continue;
      }

      fields.push(`updated_at = NOW()`);

      values.push(targetUserId);
      const userIdIndex = index++;

      let permissionCheck = "";
      const permissionParams = [];

      if (currentUser.role === "leader") {
        permissionCheck = ` AND created_by = $${index++} AND role = 'data_entry_operator' `;
        permissionParams.push(currentUser.id);
        console.log("🔒 Leader restriction active =>", currentUser.id);
      }

      const updateQuery = `
        UPDATE users
        SET ${fields.join(", ")}
        WHERE id = $${userIdIndex}
        ${permissionCheck}
        RETURNING
          id,
          username,
          email,
          mobile_no AS mobile,
          authenticated_email,
          role,
          is_active,
          parent_id,
          team_id,
          modules_code,
          permission_code,
          permissions,
          assigned_modules,
          assigned_sub_modules,
          assigned_datasets,
          module_permissions,
          updated_at
      `;

      console.log("📤 updateQuery =>", updateQuery);
      console.log("📦 update values =>", [...values, ...permissionParams]);

      const { rows } = await client.query(updateQuery, [...values, ...permissionParams]);

      console.log("📥 Update Result rows.length =>", rows.length);
      console.log("📥 Update Result rows =>", rows);

      if (!rows.length) {
        console.log("⚠️ No user row updated, skipping table copy for:", targetUserId);
        continue;
      }

      // -----------------------------------------
      // If module code applied => copy all related tables
      // -----------------------------------------
      if (moduleSourceUserId) {
        console.log("🚚 MODULE FLOW COPY START", {
          targetUserId,
          moduleSourceUserId,
        });

        await copyUserPermissions(targetUserId, moduleSourceUserId);
        await copyAssignmentsAndColumnPermissions(targetUserId, moduleSourceUserId);

        console.log("✅ MODULE FLOW COPY DONE", {
          targetUserId,
          moduleSourceUserId,
        });
      }

      // -----------------------------------------
      // If permission code applied => override/copy all related tables
      // -----------------------------------------
      if (permissionSourceUserId) {
        console.log("🚚 PERMISSION FLOW COPY START", {
          targetUserId,
          permissionSourceUserId,
        });

        await copyUserPermissions(targetUserId, permissionSourceUserId);
        await copyAssignmentsAndColumnPermissions(
          targetUserId,
          permissionSourceUserId
        );

        console.log("✅ PERMISSION FLOW COPY DONE", {
          targetUserId,
          permissionSourceUserId,
        });
      }

      // -----------------------------------------
      // Final verify before commit
      // -----------------------------------------
      const verifyUserPermissions = await client.query(
        `SELECT * FROM user_permissions WHERE user_id = $1 ORDER BY id ASC`,
        [targetUserId]
      );
      console.log(
        "🔍 verify user_permissions =>",
        verifyUserPermissions.rows.length,
        verifyUserPermissions.rows
      );

      const verifyAssignments = await client.query(
        `SELECT * FROM user_data_assignments WHERE user_id = $1 ORDER BY id ASC`,
        [targetUserId]
      );
      console.log(
        "🔍 verify user_data_assignments =>",
        verifyAssignments.rows.length,
        verifyAssignments.rows
      );

      const verifyColumns = await client.query(
        `SELECT * FROM user_column_permissions WHERE user_id = $1 ORDER BY id ASC`,
        [targetUserId]
      );
      console.log(
        "🔍 verify user_column_permissions =>",
        verifyColumns.rows.length,
        verifyColumns.rows
      );

      updatedRows.push(rows[0]);
      console.log("✅ User Updated Successfully =>", targetUserId);
    }

    await client.query("COMMIT");
    console.log("✅ TRANSACTION COMMITTED");

    return updatedRows;
  } catch (error) {
    console.log("❌ ERROR OCCURRED =>", error.message);
    console.log("❌ STACK =>", error.stack);

    await client.query("ROLLBACK");
    console.log("🔁 TRANSACTION ROLLED BACK");

    throw error;
  } finally {
    client.release();
    console.log("🔚 CLIENT RELEASED");
  }
}

async function findExistingUser({ username, email, mobile }) {
  try {
    let query;
    let params = [username, email];

    if (mobile && mobile.trim() !== '') {

      query = `
        SELECT id
        FROM users
        WHERE username = $1
           OR email = $2
           OR mobile_no = $3
        LIMIT 1
      `;
      params.push(mobile.trim());
    } else {
      query = `
        SELECT id
        FROM users
        WHERE username = $1
           OR email = $2
        LIMIT 1
      `;
    }
    const result = await pool.query(query, params);
    return result.rows.length > 0 ? result.rows[0] : null;
  } catch (error) {
    console.error("❌ findExistingUser error details:", error);
    throw new Error(`findExistingUser DB error: ${error.message}`);
  }
}

async function createUser(user) {
  try {
    const {
      username,
      email,
      authenticated_email,
      password,
      mobile,
      role,
      login_access,
      created_by
    } = user;

    await pool.query(`
      SELECT setval(
        pg_get_serial_sequence('users', 'id'),
        COALESCE((SELECT MAX(id) FROM users), 1),
        true
      )
    `);

    const query = `
      INSERT INTO users (
        username,
        email,
        authenticated_email,
        mobile_no,
        password,
        role,
        login_access,
        created_by,
        created_at,
        updated_at,
        is_active
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW(), true)
      RETURNING id
    `;

    const params = [
      username,
      email,
      authenticated_email || null,
      mobile || null,
      password,
      role,
      JSON.stringify(login_access),
      created_by || null
    ];

    const result = await pool.query(query, params);

    if (!result.rows || result.rows.length === 0) {
      throw new Error("Insert failed - no ID returned");
    }

    return { id: result.rows[0].id };
  } catch (error) {
    console.error("❌ createUser DB error details:", {
      message: error.message,
      code: error.code,
      detail: error.detail
    });
    throw new Error(`createUser DB error: ${error.message}`);
  }
}

const getAllModulesCode = async () => {
  try {
    const { rows } = await pool.query(
      "SELECT payload, code FROM modules_code"
    );
    return rows;
  } catch (error) {
    throw error;
  }
};

async function getUserDetails(userId) {
  try {
    const query = `
      SELECT
        u.id,
        u.username,
        u.email,
        u.authenticated_email,
        u.mobile_no,
        u.password,
        u.role,
        u.permissions,
        u.module_permissions,
        u.permission_code,
        u.is_active,
        u.created_at,
        u.updated_at,
        u.last_login,
        u.created_by,
        u.parent_id,
        u.team_id,
        u.last_device_info,
        u.assigned_datasets,
        u.dataset_access,
        u.data_assignment,
        u.hierarchical_data_assignment,
        u.mobile_settings,
        u.contact_permissions,
        u.accessibility_settings,
        u.email_verified,
        u.admin_verification_email,
        u.assigned_modules,
        u.modules_code,
        u.assigned_sub_modules,
        u.address,
        u.location,
        u.assignment_code,
        u.token,
        u.login_access,
        u.role_id,
        u.permission_set_id,
        u.failed_login_attempts,
        u.last_failed_login_at,
        u.account_locked_until,

        cb.username AS created_by_username,
        p.username AS parent_username

      FROM users u
      LEFT JOIN users cb ON cb.id = u.created_by
      LEFT JOIN users p ON p.id = u.parent_id
      WHERE u.id = $1
      LIMIT 1
    `;

    const result = await pool.query(query, [userId]);

    if (!result.rows.length) return null;

    const row = result.rows[0];

    const parseJSON = (value, fallback) => {
      if (value === null || value === undefined || value === "" || value === "null") {
        return fallback;
      }

      if (typeof value === "object") return value;

      try {
        return JSON.parse(value);
      } catch (error) {
        return fallback;
      }
    };

    const teamIds = row.team_id
      ? String(row.team_id)
        .split(",")
        .map((id) => Number(id.trim()))
        .filter(Boolean)
      : [];

    // let teams = [];
    // if (teamIds.length > 0) {
    //   const teamQuery = `
    //     SELECT id, name, team_code
    //     FROM teams
    //     WHERE id = ANY($1::int[])
    //     ORDER BY id ASC
    //   `;
    //   const teamResult = await pool.query(teamQuery, [teamIds]);
    //   teams = teamResult.rows || [];
    // }

    const childQuery = `
      SELECT id, username, email, mobile_no, role, is_active
      FROM users
      WHERE parent_id = $1
      ORDER BY id ASC
    `;
    const childResult = await pool.query(childQuery, [userId]);

    return {
      id: row.id,
      username: row.username || "",
      email: row.email || "",
      authenticated_email: row.authenticated_email || "",
      mobile_no: row.mobile_no || "",
      role: row.role || "",
      permissions: parseJSON(row.permissions, []),
      module_permissions: parseJSON(row.module_permissions, []),
      permission_code: row.permission_code || null,
      is_active: row.is_active,
      created_at: row.created_at,
      updated_at: row.updated_at,
      last_login: row.last_login,
      created_by: row.created_by
        ? {
          id: row.created_by,
          username: row.created_by_username || ""
        }
        : null,
      parent: row.parent_id
        ? {
          id: row.parent_id,
          username: row.parent_username || ""
        }
        : null,
      team_id: row.team_id || "",
      // teams,
      last_device_info: parseJSON(row.last_device_info, {}),
      assignedDatasets: parseJSON(row.assigned_datasets, []),
      datasetAccess: parseJSON(row.dataset_access, []),
      data_assignment: parseJSON(row.data_assignment, {}),
      hierarchical_data_assignment: parseJSON(row.hierarchical_data_assignment, {}),
      mobile_settings: parseJSON(row.mobile_settings, {}),
      contact_permissions: parseJSON(row.contact_permissions, []),
      accessibility_settings: parseJSON(row.accessibility_settings, {}),
      email_verified: row.email_verified,
      admin_verification_email: row.admin_verification_email || "",
      assignedModules: parseJSON(row.assigned_modules, []),
      modules_code: row.modules_code || "",
      assignedSubModules: parseJSON(row.assigned_sub_modules, []),
      address: row.address || "",
      location: row.location || "",
      assignment_code: row.assignment_code || "",
      token: row.token || "",
      login_access: parseJSON(row.login_access, { web: false, mobile: false }),
      role_id: row.role_id || null,
      permission_set_id: row.permission_set_id || null,
      failed_login_attempts: row.failed_login_attempts || 0,
      last_failed_login_at: row.last_failed_login_at,
      account_locked_until: row.account_locked_until,
      children: childResult.rows || []
    };
  } catch (error) {
    console.error("❌ getUserDetails DB error:", {
      message: error.message,
      code: error.code,
      detail: error.detail
    });
    throw new Error(`getUserDetails DB error: ${error.message}`);
  }
}

async function fetchTeams() {
  const { rows } = await pool.query(`
    SELECT id, name, team_code
    FROM team_master
    WHERE is_active = true
  `);

  return rows;
};

async function getUserById(id) {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM users WHERE id = $1",
      [id]
    );
    return rows[0] || null;
  } catch (error) {
    console.error("getUserById error:", error);
    throw error;
  }
}

async function updateUserById(id, data) {
  try {
    const fields = [];
    const values = [];
    let index = 1;

    for (const key in data) {
      fields.push(`${key} = $${index}`);
      values.push(
        Array.isArray(data[key]) || typeof data[key] === "object"
          ? JSON.stringify(data[key])
          : data[key]
      );
      index++;
    }

    values.push(id);

    const query = `
      UPDATE users
      SET ${fields.join(", ")}, updated_at = NOW()
      WHERE id = $${index}
      RETURNING *;
    `;

    const { rows } = await pool.query(query, values);
    return rows[0] || null;
  } catch (error) {
    console.error("updateUserById error:", error);
    throw error;
  }
}

// async function generateModulesCode(
//   modules = [],
//   datasets = [],
//   subModules = [],
//   navbarPages = [],
//   userId
// ) {
//   try {
//     const payload = {
//       assignedModules: [...modules].sort(),
//       assignedDatasets: [...datasets].sort(),
//       assignedSubModules: [...subModules].sort(),
//       assignedNavbarPages: [...navbarPages].sort(),
//     };

//     const payloadStr = JSON.stringify(payload);

//     const existing = await pool.query(
//       "SELECT code FROM modules_codes WHERE payload = $1",
//       [payloadStr]
//     );

//     if (existing.rows.length > 0) {
//       return existing.rows[0].code;
//     }

//     const code = crypto
//       .randomBytes(5)
//       .toString("hex")
//       .toUpperCase();

//     await pool.query(
//       `INSERT INTO modules_codes (code, payload, created_by)
//        VALUES ($1, $2, $3)`,
//       [code, payloadStr, userId]
//     );

//     return code;
//   } catch (error) {
//     console.error("generateModulesCode error:", error);
//     throw error;
//   }
// }

async function getUsersByIds(ids) {
  try {
    const result = await pool.query(
      `SELECT id, username, email, role
       FROM users
       WHERE id = ANY($1::int[])`,
      [ids]
    );
    return result.rows;
  } catch (error) {
    console.error("getUsersByIds error:", error);
    throw error;
  }
}

async function deleteUsersByIds(ids) {
  try {
    const result = await pool.query(
      `DELETE FROM users
       WHERE id = ANY($1::int[])`,
      [ids]
    );
    return result.rowCount || 0;
  } catch (error) {
    console.error("deleteUsersByIds error:", error);
    throw error;
  }
}

async function updateUserPasswordById(id, hashedPassword) {
  try {
    await pool.query(
      `
      UPDATE users
      SET password = $1,
          updated_at = NOW()
      WHERE id = $2
      `,
      [hashedPassword, id]
    );
    return true;
  } catch (error) {
    console.error("updateUserPasswordById error:", error);
    throw error;
  }
}

async function getPermissionModules() {
  try {
    const query = `
      SELECT 
        pm.id,
        pm.name,
        pm.code,
        pm.parent_id,
        pm.level_no,
        pm.sort_order,
        pm.icon,
        pm.route_path,
        pm.is_menu_visible,
        pa.id AS action_id,
        pa.name AS action_name,
        pa.code AS action_code,
        pa.sort_order AS action_sort_order
      FROM permission_modules pm
      LEFT JOIN permission_module_actions pma
        ON pma.module_id = pm.id
      LEFT JOIN permission_actions pa
        ON pa.id = pma.action_id
      WHERE pm.is_active = TRUE
      ORDER BY 
        pm.level_no,
        pm.sort_order,
        pm.id,
        pa.sort_order,
        pa.id
    `;

    const result = await pool.query(query);
    const rows = result.rows;

    const moduleMap = new Map();
    const rootModules = [];

    for (const row of rows) {
      if (!moduleMap.has(row.id)) {
        moduleMap.set(row.id, {
          id: row.id,
          name: row.name,
          code: row.code,
          parent_id: row.parent_id,
          level_no: row.level_no,
          sort_order: row.sort_order,
          icon: row.icon,
          route_path: row.route_path,
          is_menu_visible: row.is_menu_visible,
          actions: [],
          children: []
        });
      }

      const moduleItem = moduleMap.get(row.id);

      if (row.action_id) {
        const alreadyExists = moduleItem.actions.some(
          (action) => action.id === row.action_id
        );

        if (!alreadyExists) {
          moduleItem.actions.push({
            id: row.action_id,
            name: row.action_name,
            code: row.action_code,
            sort_order: row.action_sort_order
          });
        }
      }
    }

    for (const module of moduleMap.values()) {
      if (module.parent_id) {
        const parent = moduleMap.get(module.parent_id);
        if (parent) {
          parent.children.push(module);
        }
      } else {
        rootModules.push(module);
      }
    }

    const sortTree = (nodes) => {
      nodes.sort((a, b) => {
        if ((a.sort_order || 0) !== (b.sort_order || 0)) {
          return (a.sort_order || 0) - (b.sort_order || 0);
        }
        return a.id - b.id;
      });

      for (const node of nodes) {
        node.actions.sort((a, b) => {
          if ((a.sort_order || 0) !== (b.sort_order || 0)) {
            return (a.sort_order || 0) - (b.sort_order || 0);
          }
          return a.id - b.id;
        });

        if (node.children?.length) {
          sortTree(node.children);
        }
      }
    };

    sortTree(rootModules);

    return rootModules;
  } catch (error) {
    throw error;
  }
}

// async function assignUserPermissions({
//   user_id,
//   permissions,
//   created_by = null,
//   permission_set_name = null,
//   description = null,
// }) {
//   const client = await pool.connect();

//   try {
//     await client.query("BEGIN");

//     if (!user_id) {
//       throw new Error("user_id is required");
//     }

//     if (!Array.isArray(permissions)) {
//       throw new Error("permissions must be an array");
//     }

//     const userCheck = await client.query(
//       `SELECT id FROM users WHERE id = $1`,
//       [user_id]
//     );

//     if (!userCheck.rows.length) {
//       throw new Error("User not found");
//     }

//     // 1. remove old user permissions
//     await client.query(
//       `DELETE FROM user_permissions WHERE user_id = $1`,
//       [user_id]
//     );

//     // 2. create new permission set
//     const permissionSetRes = await client.query(
//       `INSERT INTO permission_sets (name, description, created_by, created_at, updated_at)
//          VALUES ($1, $2, $3, NOW(), NOW())
//          RETURNING id, code, name, description, created_by, created_at`,
//       [
//         permission_set_name || generatePermissionSetName(user_id),
//         description,
//         created_by,
//       ]
//     );

//     const permissionSet = permissionSetRes.rows[0];
//     const permissionSetId = permissionSet.id;

//     // 3. save permissions into user_permissions + permission_set_items
//     for (const perm of permissions) {
//       if (!perm.module_code || !Array.isArray(perm.actions)) continue;

//       const moduleRes = await client.query(
//         `SELECT id, code, name
//            FROM permission_modules
//            WHERE code = $1`,
//         [perm.module_code]
//       );

//       if (!moduleRes.rows.length) continue;

//       const moduleId = moduleRes.rows[0].id;

//       for (const actionCode of perm.actions) {
//         const actionRes = await client.query(
//           `SELECT id, code, name
//              FROM permission_actions
//              WHERE code = $1`,
//           [actionCode]
//         );

//         if (!actionRes.rows.length) continue;

//         const actionId = actionRes.rows[0].id;

//         await client.query(
//           `INSERT INTO user_permissions
//              (user_id, module_id, action_id, is_allowed, created_at, updated_at)
//              VALUES ($1, $2, $3, TRUE, NOW(), NOW())
//              ON CONFLICT (user_id, module_id, action_id)
//              DO UPDATE SET
//                is_allowed = EXCLUDED.is_allowed,
//                updated_at = NOW()`,
//           [user_id, moduleId, actionId]
//         );

//         await client.query(
//           `INSERT INTO permission_set_items
//              (permission_set_id, module_id, action_id, is_allowed, created_at)
//              VALUES ($1, $2, $3, TRUE, NOW())
//              ON CONFLICT (permission_set_id, module_id, action_id)
//              DO UPDATE SET
//                is_allowed = EXCLUDED.is_allowed`,
//           [permissionSetId, moduleId, actionId]
//         );
//       }
//     }

//     // 4. update user with permission set references
//     await client.query(
//       `UPDATE users
//          SET permission_set_id = $1,
//              permission_code = $1,
//              updated_at = NOW()
//          WHERE id = $2`,
//       [permissionSetId, user_id]
//     );

//     await client.query("COMMIT");

//     return {
//       user_id,
//       permission_set_id: permissionSetId,
//       permission_code: permissionSetId,
//       permission_set: permissionSet,
//     };
//   } catch (error) {
//     await client.query("ROLLBACK");
//     throw error;
//   } finally {
//     client.release();
//   }
// }

async function applyPermissionSetToUser({
  user_id,
  permission_set_id = null,
  permission_code = null,
}) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    if (!user_id) {
      throw new Error("user_id is required");
    }

    if (!permission_set_id && !permission_code) {
      throw new Error("permission_set_id or permission_code is required");
    }

    const userCheck = await client.query(
      `SELECT id FROM users WHERE id = $1`,
      [user_id]
    );

    if (!userCheck.rows.length) {
      throw new Error("User not found");
    }

    let setRes;

    if (permission_set_id) {
      setRes = await client.query(
        `SELECT id, code, name, description
           FROM permission_sets
           WHERE id = $1`,
        [permission_set_id]
      );
    } else {
      setRes = await client.query(
        `SELECT id, code, name, description
           FROM permission_sets
           WHERE id = $1`,
        [permission_code]
      );
    }

    if (!setRes.rows.length) {
      throw new Error("Permission set not found");
    }

    const permissionSet = setRes.rows[0];
    const permissionSetId = permissionSet.id;

    const itemsRes = await client.query(
      `SELECT module_id, action_id, is_allowed
         FROM permission_set_items
         WHERE permission_set_id = $1`,
      [permissionSetId]
    );

    // delete old user permissions
    await client.query(
      `DELETE FROM user_permissions WHERE user_id = $1`,
      [user_id]
    );

    // apply set items to user
    for (const item of itemsRes.rows) {
      await client.query(
        `INSERT INTO user_permissions
           (user_id, module_id, action_id, is_allowed, created_at, updated_at)
           VALUES ($1, $2, $3, $4, NOW(), NOW())
           ON CONFLICT (user_id, module_id, action_id)
           DO UPDATE SET
             is_allowed = EXCLUDED.is_allowed,
             updated_at = NOW()`,
        [user_id, item.module_id, item.action_id, item.is_allowed]
      );
    }

    await client.query(
      `UPDATE users
         SET permission_set_id = $1,
             permission_code = $1,
             updated_at = NOW()
         WHERE id = $2`,
      [permissionSetId, user_id]
    );

    await client.query("COMMIT");

    return {
      user_id,
      permission_set_id: permissionSetId,
      permission_code: permissionSetId,
      total_permissions: itemsRes.rows.length,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function getUserPermissions(userId) {
  const userRes = await pool.query(
    `SELECT
          id,
          username,
          email,
          role,
          role_id,
          permission_code,
          permission_set_id
       FROM users
       WHERE id = $1`,
    [userId]
  );

  if (!userRes.rows.length) {
    throw new Error("User not found");
  }

  const permissionsRes = await pool.query(
    `SELECT
          up.id,
          up.user_id,
          up.module_id,
          pm.name AS module_name,
          pm.code AS module_code,
          pm.parent_id,
          pm.level_no,
          pm.sort_order AS module_sort_order,
          up.action_id,
          pa.name AS action_name,
          pa.code AS action_code,
          pa.sort_order AS action_sort_order,
          up.is_allowed,
          up.created_at,
          up.updated_at
       FROM user_permissions up
       JOIN permission_modules pm ON pm.id = up.module_id
       JOIN permission_actions pa ON pa.id = up.action_id
       WHERE up.user_id = $1
       ORDER BY pm.level_no, pm.sort_order, pm.id, pa.sort_order, pa.id`,
    [userId]
  );

  return {
    user: userRes.rows[0],
    permissions: permissionsRes.rows,
  };
}

async function getPermissionSetDetails(permissionSetId) {
  const setRes = await pool.query(
    `SELECT
          ps.id,
          ps.code,
          ps.name,
          ps.description,
          ps.created_by,
          ps.created_at,
          ps.updated_at
       FROM permission_sets ps
       WHERE ps.id = $1`,
    [permissionSetId]
  );

  if (!setRes.rows.length) {
    throw new Error("Permission set not found");
  }

  const itemsRes = await pool.query(
    `SELECT
          psi.id,
          psi.permission_set_id,
          psi.module_id,
          pm.name AS module_name,
          pm.code AS module_code,
          pm.parent_id,
          pm.level_no,
          pm.sort_order AS module_sort_order,
          psi.action_id,
          pa.name AS action_name,
          pa.code AS action_code,
          pa.sort_order AS action_sort_order,
          psi.is_allowed,
          psi.created_at
       FROM permission_set_items psi
       JOIN permission_modules pm ON pm.id = psi.module_id
       JOIN permission_actions pa ON pa.id = psi.action_id
       WHERE psi.permission_set_id = $1
       ORDER BY pm.level_no, pm.sort_order, pm.id, pa.sort_order, pa.id`,
    [permissionSetId]
  );

  return {
    permission_set: setRes.rows[0],
    items: itemsRes.rows,
  };
}

async function getPermissionSets() {
  const result = await pool.query(
    `SELECT
          ps.id,
          ps.code,
          ps.name,
          ps.description,
          ps.created_by,
          ps.created_at,
          ps.updated_at,
          COUNT(psi.id) AS total_items
       FROM permission_sets ps
       LEFT JOIN permission_set_items psi
         ON psi.permission_set_id = ps.id
       GROUP BY ps.id
       ORDER BY ps.id DESC`
  );

  return result.rows;
}

async function getTableColumns(table) {
  try {
    const query = `
      SELECT 
        column_name,
        data_type,
        REPLACE(INITCAP(column_name), '_', ' ') AS display_name
      FROM information_schema.columns
      WHERE table_name = $1
      AND table_schema = 'public'
      AND column_name NOT IN (
        'updated_by',
        'mapping',
        'token',
        'created_at',
        'updated_at'
      )
      ORDER BY ordinal_position;
    `;

    const result = await pool.query(query, [table]);
    return result.rows;
  } catch (error) {
    throw error;
  }
}

async function getAssignmentColumnPermissions({
  user_id,
  assignment_id,
  db_table,
  owner_id
}) {
  try {
    const query = `
      SELECT
        ucp.id,
        ucp.user_id,
        ucp.assignment_id,
        ucp.db_table,
        ucp.column_name,
        ucp.can_view,
        ucp.can_mask,
        ucp.can_edit,
        ucp.can_copy,
        ucp.created_by,
        ucp.updated_by
      FROM user_column_permissions ucp
      WHERE ucp.user_id = $1
        AND ucp.assignment_id = $2
        AND ucp.db_table = $3
        AND (ucp.created_by = $4 OR ucp.updated_by = $4)
      ORDER BY ucp.column_name;
    `;

    const result = await pool.query(query, [
      user_id,
      assignment_id,
      db_table,
      owner_id
    ]);

    return result.rows;
  } catch (error) {
    throw error;
  }
}


async function assignModulesToUser({ user_id, modules }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (!user_id) throw new Error("user_id is required");

    const userRes = await client.query(
      `SELECT id, modules_code FROM users WHERE id = $1`,
      [user_id]
    );

    if (!userRes.rows.length) throw new Error("User not found");

    let modulesCode = userRes.rows[0].modules_code;
    if (!modulesCode || !String(modulesCode).trim()) {
      modulesCode = await generateModulesCode(client);
    }

    const assignedModulesSet = new Set();
    const assignedSubModulesSet = new Set();

    await client.query(`DELETE FROM user_permissions WHERE user_id = $1`, [user_id]);

    for (const item of modules) {
      if (!item.module_code) continue;

      const hierarchy = await getModuleHierarchy(client, item.module_code);
      if (!hierarchy.length) continue;

      hierarchy.forEach((mod, index) => {
        if (index === 0) {
          assignedModulesSet.add(mod.code);
        } else {
          assignedSubModulesSet.add(mod.code);
        }
      });

      const selectedModule = hierarchy[hierarchy.length - 1];

      const actions = Array.isArray(item.actions) ? item.actions : [];
      for (const actionCode of actions) {
        const actionRes = await client.query(
          `SELECT id FROM permission_actions WHERE code = $1`,
          [actionCode]
        );

        if (!actionRes.rows.length) continue;

        await client.query(
          `
          INSERT INTO user_permissions
          (user_id, module_id, action_id, is_allowed, created_at, updated_at)
          VALUES ($1, $2, $3, TRUE, NOW(), NOW())
          ON CONFLICT (user_id, module_id, action_id)
          DO UPDATE SET is_allowed = EXCLUDED.is_allowed, updated_at = NOW()
          `,
          [user_id, selectedModule.id, actionRes.rows[0].id]
        );
      }
    }

    const assignedModules = [...assignedModulesSet];
    const assignedSubModules = [...assignedSubModulesSet];

    await client.query(
      `
      UPDATE users
      SET assigned_modules = $1,
          assigned_sub_modules = $2,
          modules_code = $3,
          updated_at = NOW()
      WHERE id = $4
      `,
      [
        JSON.stringify(assignedModules),
        JSON.stringify(assignedSubModules),
        modulesCode,
        user_id
      ]
    );

    await client.query("COMMIT");

    return {
      user_id,
      modules_code: modulesCode,
      assigned_modules: assignedModules,
      assigned_sub_modules: assignedSubModules
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function applyModulesCodeToUser({ user_id, modules_code }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    if (!user_id) {
      throw new Error("user_id is required");
    }

    if (!modules_code) {
      throw new Error("modules_code is required");
    }

    const targetUserRes = await client.query(
      `SELECT id
         FROM users
         WHERE id = $1`,
      [user_id]
    );

    if (!targetUserRes.rows.length) {
      throw new Error("Target user not found");
    }

    const sourceUserRes = await client.query(
      `SELECT id, assigned_modules, modules_code
         FROM users
         WHERE modules_code = $1
         LIMIT 1`,
      [modules_code]
    );

    if (!sourceUserRes.rows.length) {
      throw new Error("Invalid modules code");
    }

    const sourceUser = sourceUserRes.rows[0];

    await client.query(
      `UPDATE users
         SET assigned_modules = $1,
             modules_code = $2,
             updated_at = NOW()
         WHERE id = $3`,
      [sourceUser.assigned_modules, sourceUser.modules_code, user_id]
    );

    await client.query("COMMIT");

    return {
      user_id,
      modules_code: sourceUser.modules_code,
      assigned_modules: sourceUser.assigned_modules
        ? JSON.parse(sourceUser.assigned_modules)
        : [],
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function getUserAssignedModules(userId) {
  const result = await pool.query(
    `SELECT
          id,
          username,
          email,
          assigned_modules,
          modules_code,
          updated_at
       FROM users
       WHERE id = $1`,
    [userId]
  );

  if (!result.rows.length) {
    throw new Error("User not found");
  }

  const user = result.rows[0];

  return {
    id: user.id,
    username: user.username,
    email: user.email,
    modules_code: user.modules_code,
    assigned_modules: user.assigned_modules
      ? JSON.parse(user.assigned_modules)
      : [],
    updated_at: user.updated_at,
  };
}


async function getUserPermissions(userId) {
  const userRes = await pool.query(
    `SELECT
          id,
          username,
          email,
          role,
          role_id,
          permission_code,
          permission_set_id
       FROM users
       WHERE id = $1`,
    [userId]
  );

  if (!userRes.rows.length) {
    throw new Error("User not found");
  }

  const permissionsRes = await pool.query(
    `SELECT
          up.id,
          up.user_id,
          up.module_id,
          pm.name AS module_name,
          pm.code AS module_code,
          pm.parent_id,
          pm.level_no,
          pm.sort_order AS module_sort_order,
          up.action_id,
          pa.name AS action_name,
          pa.code AS action_code,
          pa.sort_order AS action_sort_order,
          up.is_allowed,
          up.created_at,
          up.updated_at
       FROM user_permissions up
       JOIN permission_modules pm ON pm.id = up.module_id
       JOIN permission_actions pa ON pa.id = up.action_id
       WHERE up.user_id = $1
       ORDER BY pm.level_no, pm.sort_order, pm.id, pa.sort_order, pa.id`,
    [userId]
  );

  return {
    user: userRes.rows[0],
    permissions: permissionsRes.rows,
  };
}

async function getAssignableData(data) {

}



module.exports = {
  getAssignmentColumnPermissions,
  getUsersByIds,
  deleteUsersByIds,
  getAdminData,
  insertLoginHistory,
  getAllModulesCode,
  fetchUsers,
  fetchTeams,
  createUser,
  findExistingUser,
  generateModulesCode,
  updateUserById,
  getUserById,
  updateUserPasswordById,
  getProfileModel,
  getPermissionModules,
  getTableColumns,
  getPermissionSets,
  getPermissionSetDetails,
  getUserPermissions,
  /*assignUserPermissions*/
  applyPermissionSetToUser,

  assignModulesToUser,
  applyModulesCodeToUser,
  getUserAssignedModules,
  getAssignableData,
  getUserDetails,
  bulkUpdateUsers
};