const { pool } = require('../config/config');
const crypto = require('crypto');
const XLSX = require("xlsx");
const path = require("path");
const fs = require("fs");
const multer = require('multer')
const fastJson = require('fast-json-stringify');
const { v4: uuidv4 } = require("uuid");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { ENC_KEY } = require('../config/global');
const csvCache = new Map();
const mergedPermissionCache = new Map();

const buff_key = Buffer.from(ENC_KEY, "hex");

const votersResponseSchema = fastJson({
  type: "object",
  properties: {
    success: { type: "boolean" },

    mapping: {
      type: "object",
      additionalProperties: true
    },

    metadata: {
      type: "object",
      properties: {
        totalRecords: { type: "number" },
        currentPage: { type: "number" },
        totalPages: { type: "number" }
      },
      additionalProperties: true
    },

    voters: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: true
      }
    }
  },
  additionalProperties: true
});

function encryptPassword(plainText) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", buff_key, iv);
  const encrypted = Buffer.concat([
    cipher.update(String(plainText), "utf8"),
    cipher.final()
  ]);

  return JSON.stringify({
    iv: iv.toString("hex"),
    content: encrypted.toString("hex"),
    tag: cipher.getAuthTag().toString("hex"),
  });
}

function decryptPassword(payload) {
  const encryptedData =
    typeof payload === "string"
      ? JSON.parse(payload)
      : payload;
  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    buff_key,
    Buffer.from(encryptedData.iv, "hex")
  );
  decipher.setAuthTag(Buffer.from(encryptedData.tag, "hex"));
  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(encryptedData.content, "hex")),
    decipher.final()
  ]);
  return decrypted.toString("utf8");
}

function splitCsv(value) {
  if (value === null || value === undefined || value === '') return [];
  const key = String(value);

  if (csvCache.has(key)) {
    return csvCache.get(key);
  }

  const parsed = key
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);

  csvCache.set(key, parsed);
  return parsed;
}

function toSetFromCsv(value) {
  return new Set(splitCsv(value).map((v) => String(v).trim()));
}

function csvIncludes(csvValue, targetValue) {
  if (targetValue === null || targetValue === undefined || targetValue === '') return false;
  const values = splitCsv(csvValue);
  if (!values.length) return false;
  return values.includes(String(targetValue).trim());
}

function safeParse(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function maskValue(value) {
  if (value === null || value === undefined) return value;

  const str = String(value);
  if (!str.trim()) return str;

  return str.split(/(\s+)/).map(part => {
    if (/^\s+$/.test(part)) return part;

    if (part.length <= 4) {
      return 'x'.repeat(part.length);
    }

    if (/^\d+$/.test(part)) {
      const start = 2;
      const end = 2;
      const middle = Math.max(0, part.length - start - end);
      return part.slice(0, start) + 'x'.repeat(middle) + part.slice(-end);
    }

    return part.slice(0, 2) + 'x'.repeat(part.length - 4) + part.slice(-2);
  }).join('');
}

function prepareAssignment(assignment) {
  return {
    ...assignment,
    __data_id: assignment.data_id != null ? String(assignment.data_id) : null,
    __ac_id_set: toSetFromCsv(assignment.ac_id),
    __block_id_set: toSetFromCsv(assignment.block_id),
    __gp_ward_id_set: toSetFromCsv(assignment.gp_ward_id),
    __village_id_set: toSetFromCsv(assignment.village_id),
    __mandal_id_set: toSetFromCsv(assignment.mandal_id),
    __kendra_id_set: toSetFromCsv(assignment.kendra_id),
    __bhag_no_set: toSetFromCsv(assignment.bhag_no),
    __sec_no_set: toSetFromCsv(assignment.sec_no),
    __cast_filter_set: new Set(
      splitCsv(assignment.cast_filter).map((v) => String(v).trim().toUpperCase())
    ),
    __age_from: assignment.age_from != null && assignment.age_from !== ''
      ? Number(assignment.age_from)
      : null,
    __age_to: assignment.age_to != null && assignment.age_to !== ''
      ? Number(assignment.age_to)
      : null
  };
}

function prepareAssignments(assignments) {
  return assignments.map(prepareAssignment);
}

function getAssignedWiseFilterKey(wiseType) {
  const map = {
    block: 'block_id',
    gp_ward: 'gp_ward_id',
    village: 'village_id',
    gram: 'village_id',
    mandal: 'mandal_id',
    kendra: 'kendra_id',
    bhag: 'bhag_no',
    bhag_no: 'bhag_no',
    section: 'sec_no',
    sec_no: 'sec_no',
    ac: 'ac_no',
    ac_no: 'ac_no',
    dataid: 'data_id'
  };

  return map[wiseType] || null;
}

function buildRestrictedMappingFromAssignments(assignments) {
  const mapping = {
    village: [],
    gp_ward: [],
    block: [],
    kendra: [],
    mandal: [],
    pincode: [],
    postoff: [],
    policst: [],
    bhag_no: [],
    ru: [],
    section: [],
    castid: [],
    sex: [],
    ac: []
  };

  if (!assignments.length) return mapping;

  const wiseType = assignments[0].wise_type;

  if (wiseType === 'block') {
    const uniq = [...new Set(assignments.flatMap(x => splitCsv(x.block_id)).filter(Boolean))];
    mapping.block = uniq.map(v => ({ block_id: v, block: v }));
  }

  if (wiseType === 'gp_ward') {
    const uniq = [...new Set(assignments.flatMap(x => splitCsv(x.gp_ward_id)).filter(Boolean))];
    mapping.gp_ward = uniq.map(v => ({ gp_ward_id: v, gp_ward: v }));
  }

  if (wiseType === 'village' || wiseType === 'gram') {
    const uniq = [...new Set(assignments.flatMap(x => splitCsv(x.village_id)).filter(Boolean))];
    mapping.village = uniq.map(v => ({ village_id: v, village: v }));
  }

  if (wiseType === 'mandal') {
    const uniq = [...new Set(assignments.flatMap(x => splitCsv(x.mandal_id)).filter(Boolean))];
    mapping.mandal = uniq.map(v => ({ mandal_id: v, mandal: v }));
  }

  if (wiseType === 'kendra') {
    const uniq = [...new Set(assignments.flatMap(x => splitCsv(x.kendra_id)).filter(Boolean))];
    mapping.kendra = uniq.map(v => ({ kendra_id: v, kendra: v }));
  }

  if (wiseType === 'bhag' || wiseType === 'bhag_no') {
    mapping.bhag_no = [...new Set(assignments.flatMap(x => splitCsv(x.bhag_no)).filter(v => v != null))]
      .sort((a, b) => Number(a) - Number(b));
  }

  if (wiseType === 'section' || wiseType === 'sec_no') {
    mapping.section = [...new Set(assignments.flatMap(x => splitCsv(x.sec_no)).filter(v => v != null))]
      .sort((a, b) => Number(a) - Number(b))
      .map(v => ({ sec_no: v, section: v }));
  }

  if (wiseType === 'ac' || wiseType === 'ac_no') {
    const uniq = [...new Set(assignments.flatMap(x => splitCsv(x.ac_id)).filter(Boolean))];
    mapping.ac = uniq.map(v => ({ ac_no: v, ac_name: v }));
  }

  return mapping;
}

function buildAssignmentWhere(assignments, queryParams, filters = {}) {
  if (!assignments.length) return '';

  const groups = [];

  for (const item of assignments) {
    const andConditions = [];

    if (item.data_id && !filters.data_id) {
      queryParams.push(item.data_id);
      andConditions.push(`data_id = $${queryParams.length}`);
    }

    if (item.ac_id && !filters.ac_no && !filters.ac_id) {
      const acIds = splitCsv(item.ac_id);
      if (acIds.length) {
        queryParams.push(acIds);
        andConditions.push(`ac_no::text = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.bhag_no) {
      const bhagNos = splitCsv(item.bhag_no);
      if (bhagNos.length) {
        queryParams.push(bhagNos);
        andConditions.push(`bhag_no::text = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.sec_no) {
      const secNos = splitCsv(item.sec_no);
      if (secNos.length) {
        queryParams.push(secNos);
        andConditions.push(`sec_no::text = ANY($${queryParams.length}::text[])`);
      }
    }

    if (andConditions.length) {
      groups.push(`(${andConditions.join(' AND ')})`);
    }
  }

  if (!groups.length) return '';
  return `(${groups.join(' OR ')})`;
}

function buildAdvancedAssignmentWhere(assignments, queryParams) {
  if (!assignments.length) return '';

  const groups = [];

  for (const item of assignments) {
    const andConditions = [];

    if (item.data_id) {
      queryParams.push(Number(item.data_id));
      andConditions.push(`ed.data_id = $${queryParams.length}`);
    }

    if (item.ac_id) {
      const acIds = splitCsv(item.ac_id);
      if (acIds.length) {
        queryParams.push(acIds);
        andConditions.push(`ed.ac_no::text = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.block_id) {
      const blockIds = splitCsv(item.block_id);
      if (blockIds.length) {
        queryParams.push(blockIds);
        andConditions.push(`em.block_id = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.gp_ward_id) {
      const gpIds = splitCsv(item.gp_ward_id);
      if (gpIds.length) {
        queryParams.push(gpIds);
        andConditions.push(`em.gp_ward_id = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.village_id) {
      const villageIds = splitCsv(item.village_id);
      if (villageIds.length) {
        queryParams.push(villageIds);
        andConditions.push(`em.village_id = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.bhag_no) {
      const bhagNos = splitCsv(item.bhag_no);
      if (bhagNos.length) {
        queryParams.push(bhagNos);
        andConditions.push(`ed.bhag_no::text = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.sec_no) {
      const secNos = splitCsv(item.sec_no);
      if (secNos.length) {
        queryParams.push(secNos);
        andConditions.push(`ed.sec_no::text = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.mandal_id) {
      const mandalIds = splitCsv(item.mandal_id);
      if (mandalIds.length) {
        queryParams.push(mandalIds);
        andConditions.push(`em.mandal_id = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.kendra_id) {
      const kendraIds = splitCsv(item.kendra_id);
      if (kendraIds.length) {
        queryParams.push(kendraIds);
        andConditions.push(`em.kendra_id = ANY($${queryParams.length}::text[])`);
      }
    }

    if (item.age_from != null && item.age_from !== '') {
      queryParams.push(Number(item.age_from));
      andConditions.push(`ed.age >= $${queryParams.length}`);
    }

    if (item.age_to != null && item.age_to !== '') {
      queryParams.push(Number(item.age_to));
      andConditions.push(`ed.age <= $${queryParams.length}`);
    }

    if (item.cast_filter) {
      const castValues = splitCsv(item.cast_filter)
        .map((v) => String(v).trim().toUpperCase())
        .filter(Boolean);

      if (castValues.length) {
        queryParams.push(castValues);
        andConditions.push(`UPPER(TRIM(ed.castid)) = ANY($${queryParams.length}::text[])`);
      }
    }

    if (andConditions.length) {
      groups.push(`(${andConditions.join(' AND ')})`);
    }
  }

  if (!groups.length) return '';
  return `(${groups.join(' OR ')})`;
}

async function getUserColumnPermissions(userId, dbTable) {
  const sql = `
    SELECT
      column_name,
      can_view,
      can_mask,
      can_edit,
      can_copy
    FROM user_column_permissions
    WHERE user_id = $1
      AND db_table = $2
  `;
  const result = await pool.query(sql, [userId, dbTable]);
  return result.rows;
}

async function getUserDataAssignments(userId, dbTable, dataId = null) {
  let sql = `
    SELECT
      id,
      user_id,
      db_table,
      wise_type,
      data_id,
      block_id,
      gp_ward_id,
      village_id,
      ac_id,
      bhag_no,
      sec_no,
      mandal_id,
      kendra_id,
      age_from,
      age_to,
      cast_filter
    FROM user_data_assignments
    WHERE user_id = $1
      AND db_table = $2
      AND is_active = 1
  `;

  const params = [userId, dbTable];

  if (dataId) {
    params.push(dataId);
    sql += ` AND data_id = $${params.length}`;
  }

  sql += ` ORDER BY id ASC`;

  const result = await pool.query(sql, params);
  return result.rows;
}

function getMatchedAssignments(assignments, filters = {}) {
  return assignments.filter((item) => {
    if (filters.data_id && String(item.data_id) !== String(filters.data_id)) return false;
    if (filters.ac_id && !csvIncludes(item.ac_id, filters.ac_id)) return false;
    if (filters.ac_no && !csvIncludes(item.ac_id, filters.ac_no)) return false;
    if (filters.block && item.block_id && !csvIncludes(item.block_id, filters.block)) return false;
    if (filters.gp && item.gp_ward_id && !csvIncludes(item.gp_ward_id, filters.gp)) return false;
    if (filters.gram && item.village_id && !csvIncludes(item.village_id, filters.gram)) return false;
    if (filters.bhag_no && item.bhag_no && !csvIncludes(item.bhag_no, filters.bhag_no)) return false;
    if (filters.sec_no && item.sec_no && !csvIncludes(item.sec_no, filters.sec_no)) return false;
    if (filters.mandal && item.mandal_id && !csvIncludes(item.mandal_id, filters.mandal)) return false;
    if (filters.kendra && item.kendra_id && !csvIncludes(item.kendra_id, filters.kendra)) return false;
    return true;
  });
}

async function getUserColumnPermissionsByAssignments(userId, dbTable, assignmentIds) {
  if (!assignmentIds.length) return [];

  const sql = `
    SELECT
      assignment_id,
      column_name,
      can_view,
      can_mask,
      can_edit,
      can_copy
    FROM user_column_permissions
    WHERE user_id = $1
      AND db_table = $2
      AND assignment_id = ANY($3::int[])
  `;

  const result = await pool.query(sql, [userId, dbTable, assignmentIds]);
  return result.rows;
}

function mergeColumnPermissions(columnPermissions) {
  const map = new Map();

  for (const col of columnPermissions) {
    const existing = map.get(col.column_name);

    if (!existing) {
      map.set(col.column_name, {
        column_name: col.column_name,
        can_view: Number(col.can_view) === 1 ? 1 : 0,
        can_mask: Number(col.can_mask) === 1 ? 1 : 0,
        can_edit: Number(col.can_edit) === 1 ? 1 : 0,
        can_copy: Number(col.can_copy) === 1 ? 1 : 0
      });
      continue;
    }

    existing.can_view = Number(existing.can_view) === 1 || Number(col.can_view) === 1 ? 1 : 0;
    existing.can_mask = Number(existing.can_mask) === 1 || Number(col.can_mask) === 1 ? 1 : 0;
    existing.can_edit = Number(existing.can_edit) === 1 || Number(col.can_edit) === 1 ? 1 : 0;
    existing.can_copy = Number(existing.can_copy) === 1 || Number(col.can_copy) === 1 ? 1 : 0;
  }

  return Array.from(map.values());
}

function buildPermissionMapByAssignment(columnPermissions) {
  const map = new Map();

  for (const item of columnPermissions) {
    const key = String(item.assignment_id);
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key).push(item);
  }

  return map;
}

function mergePermissions(permissionSets) {
  const merged = new Map();

  for (const permList of permissionSets) {
    for (const perm of permList) {
      const key = perm.column_name;

      if (!merged.has(key)) {
        merged.set(key, {
          column_name: key,
          can_view: Number(perm.can_view) === 1 ? 1 : 0,
          can_mask: Number(perm.can_mask) === 1 ? 1 : 0,
          can_edit: Number(perm.can_edit) === 1 ? 1 : 0,
          can_copy: Number(perm.can_copy) === 1 ? 1 : 0
        });
      } else {
        const existing = merged.get(key);
        existing.can_view = Number(existing.can_view) === 1 || Number(perm.can_view) === 1 ? 1 : 0;
        existing.can_mask = Number(existing.can_mask) === 1 || Number(perm.can_mask) === 1 ? 1 : 0;
        existing.can_edit = Number(existing.can_edit) === 1 || Number(perm.can_edit) === 1 ? 1 : 0;
        existing.can_copy = Number(existing.can_copy) === 1 || Number(perm.can_copy) === 1 ? 1 : 0;
      }
    }
  }

  return Array.from(merged.values());
}

function getMergedPermissionsForAssignments(matchedAssignments, permissionsByAssignment) {
  const cacheKey = matchedAssignments
    .map(a => String(a.id))
    .sort()
    .join('|');

  if (mergedPermissionCache.has(cacheKey)) {
    return mergedPermissionCache.get(cacheKey);
  }

  const matchedPermissionSets = matchedAssignments
    .map((assignment) => permissionsByAssignment.get(String(assignment.id)) || [])
    .filter((arr) => arr.length > 0);

  if (!matchedPermissionSets.length) {
    mergedPermissionCache.set(cacheKey, null);
    return null;
  }

  const merged = mergePermissions(matchedPermissionSets);
  mergedPermissionCache.set(cacheKey, merged);
  return merged;
}

function compilePermissions(permissions) {
  const visibleColumns = [];
  const maskColumns = new Set();

  for (const col of permissions) {
    if (Number(col.can_view) === 1) {
      visibleColumns.push(col.column_name);
      if (Number(col.can_mask) === 1) {
        maskColumns.add(col.column_name);
      }
    }
  }

  return { visibleColumns, maskColumns };
}

function applyCompiledPermissionsToSingleRow(row, compiled) {
  const obj = {};

  for (const column of compiled.visibleColumns) {
    let value = row[column];
    if (compiled.maskColumns.has(column)) {
      value = maskValue(value);
    }
    obj[column] = value;
  }

  return obj;
}

function applyColumnPermissions(voters, columnPermissions) {
  const visibleColumns = columnPermissions
    .filter((col) => Number(col.can_view) === 1)
    .map((col) => col.column_name);

  const maskColumns = new Set(
    columnPermissions
      .filter((col) => Number(col.can_view) === 1 && Number(col.can_mask) === 1)
      .map((col) => col.column_name)
  );

  const finalRows = voters.map((row) => {
    const obj = {};
    for (const column of visibleColumns) {
      let value = row[column];
      if (maskColumns.has(column)) {
        value = maskValue(value);
      }
      obj[column] = value;
    }
    return obj;
  });

  return {
    voters: finalRows,
    visible_columns: visibleColumns
  };
}

function applyPermissionsToSingleRow(row, permissions) {
  const visibleColumns = permissions
    .filter((col) => Number(col.can_view) === 1)
    .map((col) => col.column_name);

  const maskColumns = new Set(
    permissions
      .filter((col) => Number(col.can_view) === 1 && Number(col.can_mask) === 1)
      .map((col) => col.column_name)
  );

  const obj = {};
  for (const column of visibleColumns) {
    let value = row[column];
    if (maskColumns.has(column)) {
      value = maskValue(value);
    }
    obj[column] = value;
  }
  return obj;
}

function doesRowMatchAssignment(row, assignment) {
  if (assignment.__data_id && String(row.data_id) !== assignment.__data_id) {
    return false;
  }

  const rowBlock = String(row.block_id ?? '').trim();
  const rowGp = String(row.gp_ward_id ?? '').trim();
  const rowVillage = String(row.village_id ?? '').trim();
  const rowMandal = String(row.mandal_id ?? '').trim();
  const rowKendra = String(row.kendra_id ?? '').trim();
  const rowBhag = String(row.bhag_no ?? '').trim();
  const rowSec = String(row.sec_no ?? '').trim();
  const rowAc = String(row.ac_no ?? '').trim();
  const rowCast = String(row.castid ?? '').trim().toUpperCase();
  const rowAge = row.age != null && row.age !== '' ? Number(row.age) : null;

  if (assignment.__age_from !== null && rowAge !== null && rowAge < assignment.__age_from) {
    return false;
  }

  if (assignment.__age_to !== null && rowAge !== null && rowAge > assignment.__age_to) {
    return false;
  }

  if (assignment.__cast_filter_set.size > 0 && !assignment.__cast_filter_set.has(rowCast)) {
    return false;
  }

  switch (assignment.wise_type) {
    case 'block':
      return assignment.__block_id_set.has(rowBlock);

    case 'gp_ward':
      return assignment.__block_id_set.has(rowBlock) && assignment.__gp_ward_id_set.has(rowGp);

    case 'gram':
    case 'village':
      return (
        assignment.__block_id_set.has(rowBlock) &&
        assignment.__gp_ward_id_set.has(rowGp) &&
        assignment.__village_id_set.has(rowVillage)
      );

    case 'mandal':
      return assignment.__mandal_id_set.has(rowMandal);

    case 'kendra':
      return assignment.__mandal_id_set.has(rowMandal) && assignment.__kendra_id_set.has(rowKendra);

    case 'bhag':
    case 'bhag_no':
      return assignment.__bhag_no_set.has(rowBhag);

    case 'section':
    case 'sec_no':
      return assignment.__bhag_no_set.has(rowBhag) && assignment.__sec_no_set.has(rowSec);

    case 'ac':
    case 'ac_no':
      return assignment.__ac_id_set.has(rowAc);

    case 'dataid':
      return String(row.data_id) === assignment.__data_id;

    default:
      return false;
  }
}

const formatDateReadable = (date) => {
  if (!date) return null;

  const d = new Date(date);
  const now = new Date();

  const isToday =
    d.getDate() === now.getDate() &&
    d.getMonth() === now.getMonth() &&
    d.getFullYear() === now.getFullYear();

  const yesterday = new Date();
  yesterday.setDate(now.getDate() - 1);

  const isYesterday =
    d.getDate() === yesterday.getDate() &&
    d.getMonth() === yesterday.getMonth() &&
    d.getFullYear() === yesterday.getFullYear();

  if (isToday) return "Today";
  if (isYesterday) return "Yesterday";

  return d.toLocaleDateString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
};

const ALLOWED_COLUMNS = new Set([
  "data_id_name_hi",
  "data_id_name_en",
  "ac_name_hi",
  "ac_name_en",
]);

const tableConfig = {
  dataid_importmaster: {
    filters: ["data_id", "is_active"],
    searchColumns: [
      "ac_name_hi",
      "ac_name_en",
      "pc_name_hi",
      "pc_name_en",
      "district_en",
      "district_hi",
      "party_district_hi",
      "party_district_en",
      "div_name_hi",
      "div_name_en"
    ],
    orderBy: "data_id"
  },

  eroll_castmaster: {
    filters: ["data_id"],
    searchColumns: [
      "religion_en",
      "religion_hi",
      "castcat_en",
      "castcat_hi",
      "castida_en",
      "castida_hi"
    ],
    orderBy: "data_id"
  },

  eroll_dropdown: {
    filters: ["data_id", "dropdown_name"],
    searchColumns: ["value_hi", "value_en"],
    orderBy: "dropdown_id"
  },

  eroll_yojna_master: {
    filters: ["data_id", "reg_name"],
    searchColumns: ["yojna_name"],
    orderBy: "yojna_id"
  }
};

const insertTableConfig = {
  dataid_importmaster: {
    columns: [
      "data_id",
      "data_id_name_hi",
      "data_id_name_en",
      "ac_no",
      "ac_name_en",
      "ac_name_hi",
      "pc_no",
      "pc_name_en",
      "pc_name_hi",
      "district_id",
      "district_en",
      "district_hi",
      "party_district_id",
      "party_district_hi",
      "party_district_en",
      "div_id",
      "div_name_en",
      "div_name_hi",
      "data_range",
      "is_active",
      "updated_at"
    ],
    required: ["data_id"],
    defaults: {
      ac_no: 0,
      pc_no: 0,
      district_id: 0,
      party_district_id: 0,
      div_id: 0,
      is_active: 1
    }
  },

  eroll_castmaster: {
    columns: [
      "rid",
      "religion_en",
      "religion_hi",
      "catid",
      "castcat_en",
      "castcat_hi",
      "castid",
      "castida_en",
      "castida_hi",
      "data_id"
    ],
    required: ["data_id"],
    defaults: {}
  },

  eroll_dropdown: {
    columns: [
      "dropdown_id",
      "dropdown_name",
      "value_hi",
      "value_en",
      "data_id",
      "value_id"
    ],
    required: ["data_id"],
    defaults: {}
  },

  eroll_yojna_master: {
    columns: [
      "yojna_name",
      "regid",
      "reg_name",
      "data_id",
      "is_active",
      "updated_at",
      "yojna_id"
    ],
    required: ["data_id"],
    defaults: {
      is_active: 1
    }
  }
};

const deleteTableConfig = {
  dataid_importmaster: true,
  eroll_castmaster: true,
  eroll_dropdown: true,
  eroll_yojna_master: true,
};

const patchTableConfig = {
  dataid_importmaster: [
    "data_id",
    "data_id_name_hi",
    "data_id_name_en",
    "ac_no",
    "ac_name_en",
    "ac_name_hi",
    "pc_no",
    "pc_name_en",
    "pc_name_hi",
    "district_id",
    "district_en",
    "district_hi",
    "party_district_id",
    "party_district_hi",
    "party_district_en",
    "div_id",
    "div_name_en",
    "div_name_hi",
    "data_range",
    "is_active"
  ],

  eroll_castmaster: [
    "rid",
    "religion_en",
    "religion_hi",
    "catid",
    "castcat_en",
    "castcat_hi",
    "castid",
    "castida_en",
    "castida_hi",
    "data_id"
  ],

  eroll_dropdown: [
    "dropdown_id",
    "dropdown_name",
    "value_hi",
    "value_en",
    "data_id",
    "value_id"
  ],

  eroll_yojna_master: [
    "yojna_name",
    "regid",
    "reg_name",
    "data_id",
    "is_active",
    "updated_at",
    "yojna_id"
  ]
};

const downloadTableConfig = {
  dataid_importmaster: {
    sheetName: "ImportMaster",
    query: ({ whereClause }) => `
      SELECT
        id AS "ID",
        data_id AS "DATA_ID",
        data_id_name_hi AS "DATA_ID_NAME_HI",
        data_id_name_en AS "DATA_ID_NAME_EN",
        ac_no AS "AC_NO",
        ac_name_en AS "AC_NAME_EN",
        ac_name_hi AS "AC_NAME_HI",
        pc_no AS "PC_NO",
        pc_name_en AS "PC_NAME_EN",
        pc_name_hi AS "PC_NAME_HI",
        district_id AS "DISTRICT_ID",
        district_en AS "DISTRICT_EN",
        district_hi AS "DISTRICT_HI",
        party_district_id AS "PARTY_DISTRICT_ID",
        party_district_hi AS "PARTY_DISTRICT_HI",
        party_district_en AS "PARTY_DISTRICT_EN",
        div_id AS "DIV_ID",
        div_name_en AS "DIV_NAME_EN",
        div_name_hi AS "DIV_NAME_HI",
        CASE
          WHEN data_range IS NOT NULL THEN data_range::text
          ELSE ''
        END AS "DATA_RANGE",
        is_active AS "IS_ACTIVE",
        updated_at AS "UPDATED_AT"
      FROM dataid_importmaster
      ${whereClause}
      ORDER BY id DESC
    `
  },

  eroll_castmaster: {
    sheetName: "CastMaster",
    query: ({ whereClause }) => `
      SELECT
        id AS "ID",
        rid AS "RID",
        religion_en AS "RELIGION_EN",
        religion_hi AS "RELIGION_HI",
        catid AS "CATID",
        castcat_en AS "CASTCAT_EN",
        castcat_hi AS "CASTCAT_HI",
        castid AS "CASTID",
        castida_en AS "CASTIDA_EN",
        castida_hi AS "CASTIDA_HI",
        data_id AS "DATA_ID"
      FROM eroll_castmaster
      ${whereClause}
      ORDER BY id DESC
    `
  },

  eroll_dropdown: {
    sheetName: "DropdownMaster",
    query: ({ whereClause }) => `
      SELECT
        id AS "ID",
        dropdown_id AS "DROPDOWN_ID",
        dropdown_name AS "DROPDOWN_NAME",
        value_hi AS "VALUE_HI",
        value_en AS "VALUE_EN",
        data_id AS "DATA_ID",
        value_id AS "VALUE_ID"
      FROM eroll_dropdown
      ${whereClause}
      ORDER BY id DESC
    `
  },

  eroll_yojna_master: {
    sheetName: "YojnaMaster",
    query: ({ whereClause }) => `
      SELECT
        id AS "ID",
        yojna_name AS "YOJNA_NAME",
        regid AS "REGID",
        reg_name AS "REG_NAME",
        data_id AS "DATA_ID",
        is_active AS "IS_ACTIVE",
        updated_at AS "UPDATED_AT",
        yojna_id AS "YOJNA_ID"
      FROM eroll_yojna_master
      ${whereClause}
      ORDER BY id DESC
    `
  }
};

const importCsvTableConfig = {
  dataid_importmaster: {
    tableName: "dataid_importmaster",
    sheetColumns: {
      DATA_ID: "data_id",
      DATA_ID_NAME_HI: "data_id_name_hi",
      DATA_ID_NAME_EN: "data_id_name_en",
      AC_NO: "ac_no",
      AC_NAME_EN: "ac_name_en",
      AC_NAME_HI: "ac_name_hi",
      PC_NO: "pc_no",
      PC_NAME_EN: "pc_name_en",
      PC_NAME_HI: "pc_name_hi",
      DISTRICT_ID: "district_id",
      DISTRICT_EN: "district_en",
      DISTRICT_HI: "district_hi",
      PARTY_DISTRICT_ID: "party_district_id",
      PARTY_DISTRICT_HI: "party_district_hi",
      PARTY_DISTRICT_EN: "party_district_en",
      DIV_ID: "div_id",
      DIV_NAME_EN: "div_name_en",
      DIV_NAME_HI: "div_name_hi",
      DATA_RANGE: "data_range",
      IS_ACTIVE: "is_active",
      UPDATED_AT: "updated_at"
    },
    insertColumns: [
      "data_id",
      "data_id_name_hi",
      "data_id_name_en",
      "ac_no",
      "ac_name_en",
      "ac_name_hi",
      "pc_no",
      "pc_name_en",
      "pc_name_hi",
      "district_id",
      "district_en",
      "district_hi",
      "party_district_id",
      "party_district_hi",
      "party_district_en",
      "div_id",
      "div_name_en",
      "div_name_hi",
      "data_range",
      "is_active",
      "updated_at"
    ],
    numericColumns: [
      "data_id",
      "ac_no",
      "pc_no",
      "district_id",
      "party_district_id",
      "div_id",
      "is_active"
    ],
    jsonColumns: ["data_range"],
    dateColumns: ["updated_at"],
    dataIdType: "number"
  },

  eroll_castmaster: {
    tableName: "eroll_castmaster",
    sheetColumns: {
      RID: "rid",
      RELIGION_EN: "religion_en",
      RELIGION_HI: "religion_hi",
      CATID: "catid",
      CASTCAT_EN: "castcat_en",
      CASTCAT_HI: "castcat_hi",
      CASTID: "castid",
      CASTIDA_EN: "castida_en",
      CASTIDA_HI: "castida_hi",
      DATA_ID: "data_id"
    },
    insertColumns: [
      "rid",
      "religion_en",
      "religion_hi",
      "catid",
      "castcat_en",
      "castcat_hi",
      "castid",
      "castida_en",
      "castida_hi",
      "data_id"
    ],
    numericColumns: ["data_id"],
    jsonColumns: [],
    dateColumns: [],
    dataIdType: "number"
  },

  eroll_dropdown: {
    tableName: "eroll_dropdown",
    sheetColumns: {
      DROPDOWN_ID: "dropdown_id",
      DROPDOWN_NAME: "dropdown_name",
      VALUE_HI: "value_hi",
      VALUE_EN: "value_en",
      DATA_ID: "data_id",
      VALUE_ID: "value_id"
    },
    insertColumns: [
      "dropdown_id",
      "dropdown_name",
      "value_hi",
      "value_en",
      "data_id",
      "value_id"
    ],
    numericColumns: ["dropdown_id", "data_id"],
    jsonColumns: [],
    dateColumns: [],
    dataIdType: "number"
  },

  eroll_yojna_master: {
    tableName: "eroll_yojna_master",
    sheetColumns: {
      YOJNA_NAME: "yojna_name",
      REGID: "regid",
      REG_NAME: "reg_name",
      DATA_ID: "data_id",
      IS_ACTIVE: "is_active",
      UPDATED_AT: "updated_at",
      YOJNA_ID: "yojna_id"
    },
    insertColumns: [
      "yojna_name",
      "regid",
      "reg_name",
      "data_id",
      "is_active",
      "updated_at",
      "yojna_id"
    ],
    numericColumns: ["regid", "is_active", "yojna_id"],
    jsonColumns: [],
    dateColumns: ["updated_at"],
    dataIdType: "string"
  }
};

function parseCsvBuffer(fileBuffer) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const stream = Readable.from(fileBuffer);

    stream
      .pipe(
        csv({
          mapHeaders: ({ header }) =>
            header ? header.trim().toLowerCase() : header
        })
      )
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

function normalizeImportedRow(rawRow, config) {
  const mappedRow = {};

  for (const [csvKey, dbKey] of Object.entries(config.sheetColumns)) {
    const foundKey = Object.keys(rawRow).find(
      (k) => k.trim().toUpperCase() === csvKey
    );

    if (foundKey) {
      mappedRow[dbKey] = rawRow[foundKey];
    }
  }

  for (const key of Object.keys(mappedRow)) {
    let value = mappedRow[key];

    if (typeof value === "string") {
      value = value.trim();
    }

    if (value === "") value = null;

    if (value !== null && config.numericColumns.includes(key)) {
      value = Number(value);
      if (Number.isNaN(value)) value = null;
    }

    if (value !== null && config.jsonColumns.includes(key)) {
      try {
        value = JSON.parse(value);
      } catch {
        value = null;
      }
    }

    if (value !== null && config.dateColumns.includes(key)) {
      const dt = new Date(value);
      value = isNaN(dt.getTime()) ? null : dt;
    }

    mappedRow[key] = value;
  }

  return mappedRow;
}

async function bulkInsertRows(client, tableName, insertColumns, rows) {
  if (!rows.length) return;

  const values = [];
  const placeholders = [];
  let index = 1;

  for (const row of rows) {
    const rowPlaceholders = [];

    for (const column of insertColumns) {
      let value = row[column];

      if (column === "updated_at" && !value) {
        value = new Date();
      }

      if (column === "is_active" && (value === undefined || value === null)) {
        value = 1;
      }

      if (column === "data_range" && value && typeof value === "object") {
        value = JSON.stringify(value);
      }

      rowPlaceholders.push(`$${index++}`);
      values.push(value ?? null);
    }

    placeholders.push(`(${rowPlaceholders.join(", ")})`);
  }

  const query = `
    INSERT INTO ${tableName} (${insertColumns.join(", ")})
    VALUES ${placeholders.join(", ")}
  `;

  await client.query(query, values);
}

const normalizeLikeValue = (value = "") => `%${String(value).trim()}%`;

const getArrayFilter = (value) => {
  if (Array.isArray(value)) {
    return value.map((v) => String(v).trim()).filter(Boolean);
  }
  if (value === null || value === undefined || value === "") {
    return [];
  }
  return [String(value).trim()];
};

function escapeRegex(value = "") {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildLooseSearchRegex(value = "") {
  const cleaned = String(value).trim();
  if (!cleaned) return "";

  // multiple spaces ko flexible match banao
  // "ya dav" => "ya\s*dav"
  return escapeRegex(cleaned).replace(/\s+/g, "\\s*");
}

module.exports = {
  votersResponseSchema,
  encryptPassword,
  decryptPassword,
  buildLooseSearchRegex,
  ALLOWED_COLUMNS,
  tableConfig,
  insertTableConfig,
  deleteTableConfig,
  patchTableConfig,
  downloadTableConfig,
  importCsvTableConfig,
  parseCsvBuffer,
  normalizeImportedRow,
  bulkInsertRows,
  normalizeLikeValue,
  getArrayFilter,
  formatDateReadable,
  getMergedPermissionsForAssignments,
  prepareAssignments,
  safeParse,
  applyColumnPermissions,
  buildAssignmentWhere,
  getUserDataAssignments,
  buildAdvancedAssignmentWhere,
  getUserColumnPermissions,
  getAssignedWiseFilterKey,
  buildRestrictedMappingFromAssignments,
  getMatchedAssignments,
  getUserColumnPermissionsByAssignments,
  mergeColumnPermissions,
  doesRowMatchAssignment,
  buildPermissionMapByAssignment,
  mergePermissions,
  applyPermissionsToSingleRow,
  compilePermissions,
  applyCompiledPermissionsToSingleRow,
  maskValue
};