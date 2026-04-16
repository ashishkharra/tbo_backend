const { pool } = require("../config/config");
const crypto = require("crypto");
const { encryptPassword, decryptPassword } = require("../helper/helper");


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
  try {
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

    if (currentUser.role === "leader") {
      whereConditions += `
        AND u.role = 'data_entry_operator'
        AND u.created_by = $${paramIndex}
      `;
      params.push(currentUser.id);
      paramIndex++;
    }

    if (status === "active") {
      whereConditions += `
        AND (
          COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false) = true
          OR
          COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false) = true
        )
      `;
    } else if (status === "inactive") {
      whereConditions += `
        AND NOT (
          COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false) = true
          OR
          COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false) = true
        )
      `;
    }

    if (role && role !== "all") {
      whereConditions += ` AND u.role = $${paramIndex} `;
      params.push(role);
      paramIndex++;
    }

    if (parent_id && parent_id !== "all") {
      whereConditions += ` AND u.parent_id = $${paramIndex} `;
      params.push(parent_id);
      paramIndex++;
    }

    if (username && username !== "all") {
      whereConditions += ` AND u.username = $${paramIndex} `;
      params.push(username);
      paramIndex++;
    }

    if (search && String(search).trim()) {
      whereConditions += `
        AND (
          u.username ILIKE $${paramIndex}
          OR u.email ILIKE $${paramIndex}
          OR u.mobile_no ILIKE $${paramIndex}
          OR CAST(u.id AS TEXT) ILIKE $${paramIndex}
        )
      `;
      params.push(`%${String(search).trim()}%`);
      paramIndex++;
    }

    const dataQuery = `
      SELECT
        u.id,
        u.username,
        u.email,
        u.authenticated_email,
        u.mobile_no,
        u.password,
        u.role,
        u.role_id,
        u.parent_id,
        u.created_by,
        u.modules_code,
        u.assigned_modules,
        u.assigned_sub_modules,
        u.permission_code,
        u.permissions,
        u.module_permissions,
        u.assigned_datasets,
        u.dataset_access,
        u.data_assignment,
        u.hierarchical_data_assignment,
        u.login_access,
        u.is_active AS db_is_active,
        u.created_at,
        u.updated_at,
        u.last_login,

        CASE
          WHEN
            COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false)
            OR
            COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false)
          THEN true
          ELSE false
        END AS is_active,

        COALESCE(udp.data_assignments, '[]'::json) AS user_data_assignments_data,
        COALESCE(upp.permissions, '[]'::json) AS user_permissions_data,

        json_build_object(
          'id', u.id,
          'username', u.username,
          'email', u.email,
          'authenticated_email', u.authenticated_email,
          'mobile_no', u.mobile_no,
          'role', u.role,
          'role_id', u.role_id,
          'parent_id', u.parent_id,
          'created_by', u.created_by,
          'modules_code', u.modules_code,
          'permission_code', u.permission_code,
          'assigned_modules', u.assigned_modules,
          'assigned_sub_modules', u.assigned_sub_modules,
          'permissions', u.permissions,
          'module_permissions', u.module_permissions,
          'assigned_datasets', u.assigned_datasets,
          'dataset_access', u.dataset_access,
          'data_assignment', u.data_assignment,
          'hierarchical_data_assignment', u.hierarchical_data_assignment,
          'login_access', u.login_access,
          'is_active', u.is_active,
          'created_at', u.created_at,
          'updated_at', u.updated_at,
          'last_login', u.last_login
        ) AS user_meta

      FROM users u

      LEFT JOIN LATERAL (
        SELECT json_agg(
          json_build_object(
            'id', uda.id,
            'user_id', uda.user_id,
            'db_table', uda.db_table,
            'wise_type', uda.wise_type,
            'district', uda.district,
            'ac', uda.ac,
            'pc', uda.pc,
            'party_jila', uda.party_jila,
            'data_id', uda.data_id,
            'block_id', uda.block_id,
            'gp_ward_id', uda.gp_ward_id,
            'village_id', uda.village_id,
            'ac_id', uda.ac_id,
            'bhag_no', uda.bhag_no,
            'sec_no', uda.sec_no,
            'mandal_id', uda.mandal_id,
            'kendra_id', uda.kendra_id,
            'created_by', uda.created_by,
            'updated_by', uda.updated_by,
            'created_at', uda.created_at,
            'updated_at', uda.updated_at,
            'is_active', uda.is_active,
            'age_from', uda.age_from,
            'age_to', uda.age_to,
            'cast_filter', uda.cast_filter
          )
          ORDER BY uda.id DESC
        ) AS data_assignments
        FROM user_data_assignments uda
        WHERE uda.user_id = u.id
      ) udp ON TRUE

      LEFT JOIN LATERAL (
        SELECT json_agg(
          json_build_object(
            'id', up.id,
            'user_id', up.user_id,
            'module_id', up.module_id,
            'action_id', up.action_id,
            'is_allowed', up.is_allowed,
            'created_at', up.created_at,
            'updated_at', up.updated_at
          )
          ORDER BY up.id DESC
        ) AS permissions
        FROM user_permissions up
        WHERE up.user_id = u.id
      ) upp ON TRUE

      ${whereConditions}
      ORDER BY u.id DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;

    const dataParams = [...params, limit, offset];

    const countQuery = `
      SELECT COUNT(*)::int AS total
      FROM users u
      ${whereConditions}
    `;

    const countParams = [...params];

    let [dataResult, countResult] = await Promise.all([
      pool.query(dataQuery, dataParams),
      pool.query(countQuery, countParams),
    ]);

    const parseJSONSafe = (value, fallback = null) => {
      if (value === null || value === undefined || value === "") return fallback;

      if (typeof value === "object") return value;

      try {
        return JSON.parse(value);
      } catch (error) {
        return fallback;
      }
    };

    const decryptedRows = dataResult.rows.map((row) => {
      let decryptedPassword = null;

      try {
        decryptedPassword = row.password
          ? decryptPassword(row.password)
          : null;
      } catch (err) {
        console.error(`Password decrypt failed for user ${row.id}:`, err.message);
        decryptedPassword = null;
      }

      return {
        ...row,
        password: decryptedPassword,

        assigned_modules: parseJSONSafe(row.assigned_modules, []),
        assigned_sub_modules: parseJSONSafe(row.assigned_sub_modules, []),
        permissions: parseJSONSafe(row.permissions, []),
        module_permissions: parseJSONSafe(row.module_permissions, []),
        assigned_datasets: parseJSONSafe(row.assigned_datasets, []),
        dataset_access: parseJSONSafe(row.dataset_access, []),
        data_assignment: parseJSONSafe(row.data_assignment, {}),
        hierarchical_data_assignment: parseJSONSafe(row.hierarchical_data_assignment, {}),
        login_access: parseJSONSafe(row.login_access, { web: false, mobile: false }),

        hover_details: {
          user: {
            ...parseJSONSafe(row.user_meta, {}),
            password: decryptedPassword,
            assigned_modules: parseJSONSafe(row.assigned_modules, []),
            assigned_sub_modules: parseJSONSafe(row.assigned_sub_modules, []),
            permissions: parseJSONSafe(row.permissions, []),
            module_permissions: parseJSONSafe(row.module_permissions, []),
            assigned_datasets: parseJSONSafe(row.assigned_datasets, []),
            dataset_access: parseJSONSafe(row.dataset_access, []),
            data_assignment: parseJSONSafe(row.data_assignment, {}),
            hierarchical_data_assignment: parseJSONSafe(row.hierarchical_data_assignment, {}),
            login_access: parseJSONSafe(row.login_access, { web: false, mobile: false }),
          },
          user_permissions: parseJSONSafe(row.user_permissions_data, []),
          user_data_assignments: parseJSONSafe(row.user_data_assignments_data, []),
        },
      };
    });

    return {
      rows: decryptedRows,
      total: countResult.rows[0]?.total || 0,
    };
  } catch (error) {
    console.error("fetchUsers error:", error);
    throw error;
  }
}

async function bulkUpdateUsers(users, currentUser) {
  const client = await pool.connect();

  const copyUserPermissions = async (targetUserId, sourceUserId) => {

    await client.query(
      `DELETE FROM user_permissions WHERE user_id = $1`,
      [targetUserId]
    );

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

    await client.query(
      `DELETE FROM user_data_assignments WHERE user_id = $1`,
      [targetUserId]
    );

    const sourceAssignmentsRes = await client.query(
      `
      SELECT *
      FROM user_data_assignments
      WHERE user_id = $1
      ORDER BY id ASC
      `,
      [sourceUserId]
    );


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

    for (const col of sourceColumnPermissionsRes.rows) {
      const newAssignmentId = col.assignment_id
        ? assignmentIdMap[col.assignment_id] || null
        : null;


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

    return true;
  };

  try {
    await client.query("BEGIN");

    const updatedRows = [];

    for (const user of users) {
      if (!user.id) {
        continue;
      }

      const targetUserId = Number(user.id);
      if (!targetUserId) {
        continue;
      }

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
      }

      if (user.mobile !== undefined) {
        fields.push(`mobile_no = $${index++}`);
        values.push(user.mobile);
      }

      if (user.authenticated_email !== undefined) {
        fields.push(`authenticated_email = $${index++}`);
        values.push(user.authenticated_email);
      }

      if (user.role !== undefined) {
        fields.push(`role = $${index++}`);
        values.push(user.role);
      }

      if (user.password !== undefined) {
        const plainPassword = String(user.password || "").trim();
        if (plainPassword === "") {
          throw new Error("Password cannot be empty");
        }
        const encryptedPassword = encryptPassword(plainPassword);
        fields.push(`password = $${index++}`);
        values.push(encryptedPassword);
      }

      if (user.is_active !== undefined) {
        fields.push(`is_active = $${index++}`);
        values.push(user.is_active);
      }

      if (user.login_access !== undefined) {
        fields.push(`login_access = $${index++}::jsonb`);
        values.push(
          JSON.stringify({
            web: !!user.login_access?.web,
            mobile: !!user.login_access?.mobile,
          })
        );
      }

      if (user.parent_id !== undefined) {
        fields.push(`parent_id = $${index++}`);
        values.push(user.parent_id || null);
      }

      if (user.team_id !== undefined) {
        fields.push(`team_id = $${index++}`);
        values.push(user.team_id || null);
      }

      if (user.permissions !== undefined) {
        fields.push(`permissions = $${index++}`);
        values.push(
          Array.isArray(user.permissions)
            ? JSON.stringify(user.permissions)
            : user.permissions
        );
      }

      if (user.assignedModules !== undefined) {
        fields.push(`assigned_modules = $${index++}`);
        values.push(
          Array.isArray(user.assignedModules)
            ? JSON.stringify(user.assignedModules)
            : user.assignedModules
        );
      }

      if (user.assignedSubModules !== undefined) {
        fields.push(`assigned_sub_modules = $${index++}`);
        values.push(
          Array.isArray(user.assignedSubModules)
            ? JSON.stringify(user.assignedSubModules)
            : user.assignedSubModules
        );
      }

      if (user.assignedDatasets !== undefined) {
        fields.push(`assigned_datasets = $${index++}`);
        values.push(
          Array.isArray(user.assignedDatasets)
            ? JSON.stringify(user.assignedDatasets)
            : user.assignedDatasets
        );
      }

      // -----------------------------
      // MODULE CODE COPY
      // -----------------------------
      if (user.modules_code !== undefined && String(user.modules_code).trim() !== "") {
        const sourceModuleCode = String(user.modules_code).trim();

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

        if (!moduleSourceRes.rows.length) {
          throw new Error(`Invalid modules_code: ${sourceModuleCode}`);
        }

        const sourceUser = moduleSourceRes.rows[0];
        moduleSourceUserId = sourceUser.id;

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
          login_access,
          modules_code,
          permission_code,
          permissions,
          assigned_modules,
          assigned_sub_modules,
          assigned_datasets,
          module_permissions,
          updated_at
      `;

      const { rows } = await client.query(updateQuery, [...values, ...permissionParams]);

      if (!rows.length) {
        continue;
      }

      // -----------------------------------------
      // If module code applied => copy all related tables
      // -----------------------------------------
      if (moduleSourceUserId) {
        await copyUserPermissions(targetUserId, moduleSourceUserId);
        await copyAssignmentsAndColumnPermissions(targetUserId, moduleSourceUserId);
      }

      // -----------------------------------------
      // If permission code applied => override/copy all related tables
      // -----------------------------------------
      if (permissionSourceUserId) {
        await copyUserPermissions(targetUserId, permissionSourceUserId);
        await copyAssignmentsAndColumnPermissions(
          targetUserId,
          permissionSourceUserId
        );
      }

      // -----------------------------------------
      // Final verify before commit
      // -----------------------------------------
      const verifyUserPermissions = await client.query(
        `SELECT * FROM user_permissions WHERE user_id = $1 ORDER BY id ASC`,
        [targetUserId]
      );

      const verifyAssignments = await client.query(
        `SELECT * FROM user_data_assignments WHERE user_id = $1 ORDER BY id ASC`,
        [targetUserId]
      );

      const verifyColumns = await client.query(
        `SELECT * FROM user_column_permissions WHERE user_id = $1 ORDER BY id ASC`,
        [targetUserId]
      );

      updatedRows.push(rows[0]);
    }

    await client.query("COMMIT");

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

async function getUserDetails(userId, currentUser) {
  try {
    const isCurrentUserSuperAdmin = currentUser?.role === "super_admin";

    let whereClause = `WHERE u.id = $1`;
    const params = [userId];

    if (!isCurrentUserSuperAdmin) {
      whereClause += ` AND u.role <> $2`;
      params.push("super_admin");
    }

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
      ${whereClause}
      LIMIT 1
    `;

    const result = await pool.query(query, params);

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

    let decryptedPassword = "";

    if (row.password) {
      try {
        decryptedPassword = decryptPassword(row.password);
      } catch (error) {
        console.error("❌ Password decrypt error:", error.message);
        decryptedPassword = "";
      }
    }

    const childQuery = `
      SELECT id, username, email, mobile_no, role, is_active
      FROM users
      WHERE parent_id = $1
      ${!isCurrentUserSuperAdmin ? `AND role <> 'super_admin'` : ""}
      ORDER BY id ASC
    `;
    const childResult = await pool.query(childQuery, [userId]);

    return {
      id: row.id,
      username: row.username || "",
      email: row.email || "",
      authenticated_email: row.authenticated_email || "",
      mobile_no: row.mobile_no || "",
      password: decryptedPassword,
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
      "SELECT username FROM users WHERE id = $1",
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
      let value = data[key];
      if (key === "password") {
        const plainPassword = String(value || "").trim();
        if (!plainPassword) {
          throw new Error("Password cannot be empty");
        }
        value = encryptPassword(plainPassword);
      }

      fields.push(`${key} = $${index}`);
      values.push(
        Array.isArray(value) || typeof value === "object"
          ? JSON.stringify(value)
          : value
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

async function unlinkUser(params) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const userId = Number(params?.id || params?.user_id);

    if (!userId || Number.isNaN(userId)) {
      await client.query("ROLLBACK");
      return {
        success: false,
        message: "Valid user id is required",
      };
    }

    const userRes = await client.query(
      `
      SELECT
        id,
        email,
        authenticated_email,
        mobile_no,
        token
      FROM users
      WHERE id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (userRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return {
        success: false,
        message: "User not found",
      };
    }

    const user = userRes.rows[0];

    // get all currently active sessions first
    const activeSessionsRes = await client.query(
      `
      SELECT
        id,
        session_token,
        platform,
        device_type,
        device_id,
        generated_device_id,
        device_name,
        browser,
        os,
        ip_address,
        user_agent,
        personal_email,
        authenticated_email,
        fingerprint_hash
      FROM user_sessions
      WHERE user_id = $1
        AND is_active = true
        AND COALESCE(is_logged_out, false) = false
      `,
      [userId]
    );

    const activeSessions = activeSessionsRes.rows;

    // logout all active sessions
    const sessionUpdateRes = await client.query(
      `
      UPDATE user_sessions
      SET
        is_active = false,
        is_logged_out = true,
        logout_reason = 'USER_UNLINKED_BY_ADMIN',
        logged_out_at = NOW(),
        updated_at = NOW(),
        expires_at = NOW()
      WHERE user_id = $1
        AND is_active = true
        AND COALESCE(is_logged_out, false) = false
      `,
      [userId]
    );

    let revokedDevices = 0;

    // revoke trusted device records for the same active devices
    for (const session of activeSessions) {
      if (!session.generated_device_id || !session.platform) {
        continue;
      }

      const revokeDeviceRes = await client.query(
        `
        UPDATE user_verified_devices
        SET
          is_verified = false,
          is_revoked = true,
          revoked_at = NOW(),
          revoked_reason = 'USER_UNLINKED_BY_ADMIN',
          verification_status = 'REVOKED',
          updated_at = NOW()
        WHERE user_id = $1
          AND generated_device_id = $2
          AND platform = $3
          AND COALESCE(is_revoked, false) = false
        `,
        [userId, session.generated_device_id, session.platform]
      );

      revokedDevices += revokeDeviceRes.rowCount;

      await client.query(
        `
        INSERT INTO user_session_logs (
          user_id,
          session_token,
          event_type,
          platform,
          device_id,
          device_name,
          browser,
          os,
          ip_address,
          user_agent,
          personal_email,
          authenticated_email,
          message,
          meta_data,
          created_at
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW()
        )
        `,
        [
          userId,
          session.session_token,
          "DEVICE_UNVERIFIED_BY_ADMIN",
          session.platform || null,
          session.generated_device_id || session.device_id || null,
          session.device_name || null,
          session.browser || null,
          session.os || null,
          session.ip_address || null,
          session.user_agent || null,
          session.personal_email || user.email || null,
          session.authenticated_email || user.authenticated_email || null,
          "User was unlinked by admin. Session expired and trusted device revoked.",
          JSON.stringify({
            reason: "USER_UNLINKED_BY_ADMIN",
            session_id: session.id,
            session_token: session.session_token,
            platform: session.platform || null,
            device_type: session.device_type || null,
            device_id: session.device_id || null,
            generated_device_id: session.generated_device_id || null,
            fingerprint_hash: session.fingerprint_hash || null,
            action: "UNLINK_USER_AND_REVOKE_DEVICE",
            next_login_requires_device_otp: true,
          }),
        ]
      );
    }

    await client.query("COMMIT");

    return {
      success: true,
      message: "User unlinked successfully",
      data: {
        expired_sessions: sessionUpdateRes.rowCount,
        revoked_devices: revokedDevices,
      },
    };
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("unlinkUser error:", error);
    return {
      success: false,
      message: "Failed to unlink user",
      error: error.message,
    };
  } finally {
    client.release();
  }
}



module.exports = {
  unlinkUser,
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