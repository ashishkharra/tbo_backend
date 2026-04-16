const { pool } = require('../config/config')
const XLSX = require("xlsx");
const path = require("path");
const fs = require("fs");
const multer = require('multer')
const { v4: uuidv4 } = require("uuid");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { getUserDataAssignments, buildAdvancedAssignmentWhere, doesRowMatchAssignment, buildPermissionMapByAssignment, getUserColumnPermissionsByAssignments, prepareAssignments, getMergedPermissionsForAssignments, compilePermissions, applyCompiledPermissionsToSingleRow, bulkInsertRows, normalizeImportedRow, parseCsvBuffer, importCsvTableConfig, downloadTableConfig, patchTableConfig, deleteTableConfig, insertTableConfig, tableConfig, ALLOWED_COLUMNS, getArrayFilter, normalizeLikeValue, buildLooseSearchRegex } = require('../helper/helper.js');


async function refreshGlobalCache() {
  const { initGlobalCache } = require('../controllers/dataId.controller.js');
  return initGlobalCache();
}


module.exports = {

  getAcList: async () => {
    const query = `
    SELECT DISTINCT
      ac_no,
      ac_name_hi
    FROM dataid_importmaster
    WHERE is_active = 0
      OR ac_no IS NOT NULL
      OR ac_no <> 0
      OR ac_name_hi IS NOT NULL
    ORDER BY ac_no
  `;

    const { rows } = await pool.query(query);
    return rows;
  },

  getDataIdsByAc: async (ac_no, ac_name_hi) => {
    const query = `
    SELECT
      data_id,
      data_id_name_hi,
      data_id_name_en
    FROM dataid_importmaster
    WHERE is_active = 0
      AND ac_no = $1
      AND ac_name_hi = $2
    ORDER BY data_id
  `;

    const { rows } = await pool.query(query, [ac_no, ac_name_hi]);
    return rows;
  },

  updateDataIdByKeys: async (data_id, ac_no, fields) => {
    try {
      const keys = Object.keys(fields).filter(k => ALLOWED_COLUMNS.has(k));

      if (keys.length === 0) return false;

      const values = [];
      const setClauses = keys.map((key, index) => {
        values.push(fields[key]);
        return `"${key}" = $${index + 1}`;
      });

      setClauses.push(`is_active = 1`);
      setClauses.push(`updated_at = CURRENT_TIMESTAMP`);

      values.push(data_id);
      values.push(ac_no);

      const sql = `
      UPDATE dataid_importmaster
      SET ${setClauses.join(", ")}
      WHERE data_id = $${values.length - 1}
        AND ac_no = $${values.length}
      RETURNING id
    `;

      const result = await pool.query(sql, values);

      return result.rowCount > 0;

    } catch (err) {
      throw err;
    }
  },

  getDataIdsAllActiveRows: async (page = 1, limit = 10) => {
    const offset = (page - 1) * limit;

    const query = `
    SELECT
      data_id,
      ac_no,
      data_id_name_hi,
      data_id_name_en,
      is_active,
      data_range,
      updated_at
    FROM dataid_importmaster
    WHERE is_active = 1
    ORDER BY data_id
    LIMIT $1 OFFSET $2
  `;

    const { rows } = await pool.query(query, [limit, offset]);

    return {
      page,
      limit,
      count: rows.length,
      data: rows
    };
  },

  checkExistdataIdAcNo: async (ac_no, data_id) => {
    try {
      const query = `
            SELECT 1 
            FROM dataid_importmaster 
            WHERE ac_no = $1 AND data_id = $2 
            LIMIT 1
        `;
      const result = await pool.query(query, [ac_no, data_id]);

      return result.rowCount > 0;
    } catch (error) {
      console.error("Database check error:", error);
      throw error;
    }
  },

  getAllForCache: async () => {
    try {
      const query = `
    SELECT 
        id, data_id, data_id_name_hi,
        ac_no, ac_name_hi,
        pc_no, pc_name_hi,
        district_id, district_hi, party_district_id, party_district_hi,
        div_id, div_name_hi,
        data_range, is_active
    FROM dataid_importmaster
    WHERE is_active = 1
    ORDER BY data_id, ac_no, pc_no, district_id;
`;
      const { rows } = await pool.query(query);

      return rows;
    } catch (error) {
      console.error("Model Error (getAllForCache):", error);
      throw error;
    }
  },

  filterVoters: async (user, filters, limit = 100, offset = 0) => {
    try {
      const {
        data_id,
        ac_no,
        ageFrom,
        ageTo,
        cast_filter,
        castid
      } = filters;

      let queryParams = [];
      let conditions = [];

      const isSuperUser =
        user.role === 'admin' ||
        user.role === 'super_admin' ||
        user.role === 'Admin' ||
        user.role === 'Super Admin';

      const dbTable = 'eroll_db';

      if (data_id) {
        queryParams.push(data_id);
        conditions.push(`ed.data_id = $${queryParams.length}`);
      }

      if (ac_no) {
        queryParams.push(ac_no);
        conditions.push(`ed.ac_no = $${queryParams.length}`);
      }

      if (ageFrom !== undefined && ageFrom !== null && ageFrom !== '') {
        queryParams.push(Number(ageFrom));
        conditions.push(`ed.age >= $${queryParams.length}`);
      }

      if (ageTo !== undefined && ageTo !== null && ageTo !== '') {
        queryParams.push(Number(ageTo));
        conditions.push(`ed.age <= $${queryParams.length}`);
      }

      const rawCastFilter = cast_filter ?? castid;
      if (rawCastFilter !== undefined && rawCastFilter !== null && rawCastFilter !== '') {
        const castValues = String(rawCastFilter)
          .split(',')
          .map((v) => String(v).trim().toUpperCase())
          .filter(Boolean);

        if (castValues.length) {
          queryParams.push(castValues);
          conditions.push(`UPPER(TRIM(ed.castid)) = ANY($${queryParams.length}::text[])`);
        }
      }

      let dataAssignments = [];
      let rawColumnPermissions = [];

      if (!isSuperUser) {
        dataAssignments = await getUserDataAssignments(user.id, dbTable, data_id);

        const assignmentWhere = buildAdvancedAssignmentWhere(dataAssignments, queryParams);

        if (assignmentWhere) {
          conditions.push(assignmentWhere);
        }

        const assignmentIds = dataAssignments.map(x => x.id);

        rawColumnPermissions = await getUserColumnPermissionsByAssignments(
          user.id,
          dbTable,
          assignmentIds
        );

        if (!rawColumnPermissions.length) {
          return {
            mapping: {},
            voters: [],
            total: 0,
            visible_columns: []
          };
        }
      }

      const whereClause = conditions.length
        ? `WHERE ${conditions.join(" AND ")}`
        : "";

      const mappingSql = `
      SELECT DISTINCT
        em.ru,
        em.village_id, em.village,
        em.gp_ward_id, em.gp_ward,
        em.block_id, em.block,
        em.kendra_id, em.kendra,
        em.mandal_id, em.mandal,
        em.pincode_id, em.pincode,
        em.postoff_id, em.postoff,
        em.policst_id, em.policst,
        em.bhag_no,
        em.sec_no,
        em.section
      FROM eroll_mapping em
      ${data_id ? `WHERE em.data_id = $1` : ``}
      ORDER BY em.bhag_no ASC, em.sec_no ASC
    `;

      const mappingRes = await pool.query(mappingSql, data_id ? [data_id] : []);
      const rows = mappingRes.rows;
      const transformedMapping = {};

      console.time("caste");

      const casteParams = [...queryParams];

      const casteConditions = [...conditions];
      casteConditions.push(`ed.castid IS NOT NULL`);
      casteConditions.push(`TRIM(ed.castid) <> ''`);

      const casteWhereClause = casteConditions.length
        ? `WHERE ${casteConditions.join(" AND ")}`
        : "";

      const casteSql = `
  SELECT
    UPPER(TRIM(ed.castid)) AS castid,
    ed.sex
  FROM eroll_db ed
  ${casteWhereClause}
  GROUP BY UPPER(TRIM(ed.castid)), ed.sex
  ORDER BY UPPER(TRIM(ed.castid)) ASC
`;

      const casteRes = await pool.query(casteSql, casteParams);

      console.timeEnd("caste");

      transformedMapping.castid = [
        ...new Set(casteRes.rows.map(r => r.castid).filter(Boolean))
      ].sort((a, b) => String(a).localeCompare(String(b)));

      transformedMapping.sex = [
        ...new Set(casteRes.rows.map(r => r.sex).filter(v => v != null && String(v).trim() !== ''))
      ].sort((a, b) => String(a).localeCompare(String(b)));

      const buildPairArray = (idKey, nameKey) => {
        const map = new Map();

        rows.forEach(row => {
          if (row[idKey] != null && row[nameKey] != null) {
            map.set(row[idKey], {
              [idKey]: row[idKey],
              [nameKey]: row[nameKey]
            });
          }
        });

        return Array.from(map.values()).sort((a, b) =>
          String(a[nameKey]).localeCompare(String(b[nameKey]))
        );
      };

      if (rows.length > 0) {
        transformedMapping.village = buildPairArray("village_id", "village");
        transformedMapping.gp_ward = buildPairArray("gp_ward_id", "gp_ward");
        transformedMapping.block = buildPairArray("block_id", "block");
        transformedMapping.kendra = buildPairArray("kendra_id", "kendra");
        transformedMapping.mandal = buildPairArray("mandal_id", "mandal");
        transformedMapping.pincode = buildPairArray("pincode_id", "pincode");
        transformedMapping.postoff = buildPairArray("postoff_id", "postoff");
        transformedMapping.policst = buildPairArray("policst_id", "policst");

        transformedMapping.bhag_no = [...new Set(rows.map(r => r.bhag_no).filter(v => v != null))]
          .sort((a, b) => a - b);

        transformedMapping.ru = [...new Set(rows.map(r => r.ru).filter(v => v != null))]
          .sort((a, b) => a - b);

        const secMap = new Map();

        rows.forEach(row => {
          if (row.sec_no != null) {
            secMap.set(row.sec_no, {
              sec_no: row.sec_no,
              section: row.section
            });
          }
        });

        transformedMapping.section = Array.from(secMap.values())
          .sort((a, b) => a.sec_no - b.sec_no);
      }

      console.time('voters')
      const voterSql = `
        SELECT
          ed.*,
          em.block_id,
          em.gp_ward_id,
          em.village_id,
          em.mandal_id,
          em.kendra_id,
          em.block,
          em.gp_ward,
          em.village,
          em.mandal,
          em.kendra,
          em.section,

          COALESCE(ecm.castida_hi, '') AS cast_id,
          COALESCE(ecm.castida_hi, '') AS caste,
          COALESCE(ecm.castcat_en, '') AS cast_cat_name,
          COALESCE(ecm.religion_hi, '') AS religion,

          TO_CHAR(ed.dob, 'DD-MM-YYYY') as dob,
          TO_CHAR(ed.update_by, 'DD-MM-YYYY HH24:MI') as update_by
        FROM eroll_db ed
        LEFT JOIN eroll_mapping em
          ON ed.data_id = em.data_id
        AND ed.ac_no = em.ac_id
        AND ed.bhag_no = em.bhag_no
        AND ed.sec_no = em.sec_no
        LEFT JOIN eroll_castmaster ecm
          ON ecm.data_id = ed.data_id
        AND UPPER(TRIM(ecm.castid)) = UPPER(TRIM(ed.castid))
        ${whereClause}
        ORDER BY ed.ac_no ASC, ed.bhag_no ASC, ed.sec_no ASC, ed.vsno ASC
        LIMIT $${queryParams.length + 1}
        OFFSET $${queryParams.length + 2}
      `;

      const { rows: votersRaw } = await pool.query(
        voterSql,
        [...queryParams, limit, offset]
      );
      console.timeEnd('voters')

      console.time('count')

      let totalCount = 0;

      const isSimpleCount =
        data_id &&
        !ac_no &&
        !ageFrom &&
        !ageTo &&
        !cast_filter &&
        !castid &&
        (!conditions || conditions.length === 1);

      if (isSimpleCount) {
        const countCacheRes = await pool.query(
          `SELECT total_count FROM eroll_db_counts WHERE data_id = $1 LIMIT 1`,
          [data_id]
        );

        if (countCacheRes.rowCount > 0) {
          totalCount = Number(countCacheRes.rows[0].total_count);
        } else {
          const countSql = `
            SELECT COUNT(*)::BIGINT AS count
            FROM eroll_db ed
            LEFT JOIN eroll_mapping em
            ON ed.data_id = em.data_id
            AND ed.ac_no = em.ac_id
            AND ed.bhag_no = em.bhag_no
            AND ed.sec_no = em.sec_no
            ${whereClause}
          `;

          const countRes = await pool.query(countSql, queryParams);
          totalCount = Number(countRes.rows[0].count);

          await pool.query(
            `
              INSERT INTO eroll_db_counts (data_id, total_count)
              VALUES ($1, $2)
              ON CONFLICT (data_id)
              DO UPDATE SET total_count = EXCLUDED.total_count,
              updated_at = NOW()
            `,
            [data_id, totalCount]
          );
        }
      } else {
        const countSql = `
          SELECT COUNT(*)::BIGINT AS count
          FROM eroll_db ed
          LEFT JOIN eroll_mapping em
          ON ed.data_id = em.data_id
          AND ed.ac_no = em.ac_id
          AND ed.bhag_no = em.bhag_no
          AND ed.sec_no = em.sec_no
          ${whereClause}
        `;

        const countRes = await pool.query(countSql, queryParams);
        totalCount = Number(countRes.rows[0].count);
      }

      console.timeEnd('count')


      let finalVoters = votersRaw;
      let visible_columns = [];

      console.time('permission')

      if (!isSuperUser) {
        const preparedAssignments = prepareAssignments(dataAssignments);
        const permissionsByAssignment = buildPermissionMapByAssignment(rawColumnPermissions);
        const compiledPermissionsCache = new Map();

        finalVoters = votersRaw.map((row) => {
          const matchedAssignments = [];

          for (const assignment of preparedAssignments) {
            if (doesRowMatchAssignment(row, assignment)) {
              matchedAssignments.push(assignment);
            }
          }

          if (!matchedAssignments.length) {
            return null;
          }

          const mergedPermissions = getMergedPermissionsForAssignments(
            matchedAssignments,
            permissionsByAssignment
          );

          if (!mergedPermissions) {
            return null;
          }

          const permKey = matchedAssignments
            .map(a => String(a.id))
            .sort()
            .join('|');

          let compiled = compiledPermissionsCache.get(permKey);

          if (!compiled) {
            compiled = compilePermissions(mergedPermissions);
            compiledPermissionsCache.set(permKey, compiled);
          }

          return applyCompiledPermissionsToSingleRow(row, compiled);
        }).filter(Boolean);

        visible_columns = [...new Set(finalVoters.flatMap(row => Object.keys(row)))];
      } else {
        visible_columns = Object.keys(votersRaw[0] || {});
      }

      console.timeEnd('permission')

      return {
        mapping: transformedMapping,
        voters: finalVoters,
        total: totalCount,
        visible_columns
      };
    } catch (error) {
      console.error(error);
      throw error;
    }
  },

  applyAdvancedFilter: async (user, filters, limit = 100, offset = 0) => {
    try {
      const isValid = (val) =>
        val !== undefined &&
        val !== null &&
        val !== "" &&
        val !== "null" &&
        val !== "undefined";

      const isSuperUser =
        user.role === "admin" ||
        user.role === "super_admin" ||
        user.role === "Admin" ||
        user.role === "Super Admin";

      const dbTable = "eroll_db";

      let dataAssignments = [];
      let rawColumnPermissions = [];

      const conditions = [];
      const params = [];

      const masterFilters = [
        { key: "data_id", column: "ed.data_id" },
        { key: "ac_no", column: "ed.ac_no" }
      ];

      for (const filter of masterFilters) {
        if (isValid(filters[filter.key])) {
          params.push(Number(filters[filter.key]));
          conditions.push(`${filter.column} = $${params.length}`);
        }
      }

      const ignoreKeys = [
        "page",
        "limit",
        "pc_no",
        "district_id",
        "party_district_id"
      ];

      const masterFilterKeys = masterFilters.map((f) => f.key);

      const mappingFields = [
        "gram",
        "gp",
        "block",
        "mandal",
        "kendra",
        "postOffice",
        "pinCode",
        "policeStation"
      ];

      for (const [key, value] of Object.entries(filters)) {
        if (ignoreKeys.includes(key)) continue;
        if (masterFilterKeys.includes(key)) continue;
        if (!isValid(value)) continue;

        // ye special filters alag handle honge
        if (
          key === "ageFrom" ||
          key === "ageTo" ||
          key === "cast_filter" ||
          key === "castid" ||
          key === "castId"
        ) {
          continue;
        }

        let column;

        if (mappingFields.includes(key)) {
          switch (key) {
            case "gram":
              column = "em.village_id";
              break;
            case "gp":
              column = "em.gp_ward_id";
              break;
            case "block":
              column = "em.block_id";
              break;
            case "mandal":
              column = "em.mandal_id";
              break;
            case "kendra":
              column = "em.kendra_id";
              break;
            case "postOffice":
              column = "em.postoff_id";
              break;
            case "pinCode":
              column = "em.pincode_id";
              break;
            case "policeStation":
              column = "em.policst_id";
              break;
            default:
              column = `em.${key}`;
          }
        } else {
          const columnMap = {
            gender: "ed.sex",
            sex: "ed.sex",
            castId: "ed.castid",
            castid: "ed.castid",
            cast: "ed.castid",
            name: "ed.vname",
            surname: "ed.surname",
            mobile: "ed.phone1",
            dob: "ed.dob",
            bhag_no: "ed.bhag_no",
            bhagNo: "ed.bhag_no",
            bhag: "ed.bhag_no",
            sec_no: "ed.sec_no",
            sectionNo: "ed.sec_no",
            section: "ed.sec_no",
            lbt: "em.ru",
            ru: "em.ru",
            hno: "ed.hno",
            phone1: "ed.phone1",
            vname: "ed.vname",
            aadhar: "ed.aadhar_no",
            profession_name: "ed.proff_id",
            edu: "ed.edu_id",
            mukhiya: "ed.hof"
          };

          column = columnMap[key] || `ed.${key}`;
        }

        const numericFields = [
          "bhag_no",
          "sec_no",
          "data_id",
          "ac_no",
          "ru",
          "lbt",
          "bhagNo",
          "bhag",
          "sectionNo",
          "section",
          "mukhiya"
        ];

        if (numericFields.includes(key)) {
          params.push(Number(value));
        } else {
          params.push(value);
        }

        conditions.push(`${column} = $${params.length}`);
      }

      // age range
      if (isValid(filters.ageFrom) && isValid(filters.ageTo)) {
        params.push(Number(filters.ageFrom));
        params.push(Number(filters.ageTo));
        conditions.push(`ed.age BETWEEN $${params.length - 1} AND $${params.length}`);
      } else if (isValid(filters.ageFrom)) {
        params.push(Number(filters.ageFrom));
        conditions.push(`ed.age >= $${params.length}`);
      } else if (isValid(filters.ageTo)) {
        params.push(Number(filters.ageTo));
        conditions.push(`ed.age <= $${params.length}`);
      }

      // cast filter: MU,O style comma-separated values
      const rawCastFilter =
        filters.cast_filter ?? filters.castid ?? filters.castId ?? null;

      if (isValid(rawCastFilter)) {
        const castValues = String(rawCastFilter)
          .split(",")
          .map((v) => String(v).trim().toUpperCase())
          .filter(Boolean);

        if (castValues.length) {
          params.push(castValues);
          conditions.push(`UPPER(TRIM(ed.castid)) = ANY($${params.length}::text[])`);
        }
      }

      if (!isSuperUser) {
        dataAssignments = await getUserDataAssignments(
          user.id,
          dbTable,
          isValid(filters.data_id) ? filters.data_id : null
        );

        if (!dataAssignments.length) {
          return {
            voters: [],
            total: 0,
            mapping: {},
            visible_columns: []
          };
        }

        const assignmentWhere = buildAdvancedAssignmentWhere(dataAssignments, params);

        if (assignmentWhere) {
          conditions.push(assignmentWhere);
        }

        const assignmentIds = dataAssignments.map((x) => x.id);

        rawColumnPermissions = await getUserColumnPermissionsByAssignments(
          user.id,
          dbTable,
          assignmentIds
        );

        if (!rawColumnPermissions.length) {
          return {
            voters: [],
            total: 0,
            mapping: {},
            visible_columns: []
          };
        }
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const baseQuery = `
      WITH filtered_voters AS (
        SELECT
          ed.*,
          em.block_id,
          em.gp_ward_id,
          em.village_id,
          em.mandal_id,
          em.kendra_id,
          em.block,
          em.gp_ward,
          em.village,
          em.mandal,
          em.kendra,
          em.section AS mapping_section,
          em.pincode_id,
          em.pincode,
          em.postoff_id,
          em.postoff,
          em.policst_id,
          em.policst,
          em.ru
        FROM eroll_db ed
        JOIN dataid_importmaster dm
          ON ed.data_id = dm.data_id
        JOIN eroll_mapping em
          ON ed.data_id = em.data_id
         AND ed.ac_no = em.ac_id
         AND ed.bhag_no = em.bhag_no
         AND ed.sec_no = em.sec_no
        ${whereClause}
      )
    `;

      const voterSql = `
      ${baseQuery}
      SELECT *,
        TO_CHAR(dob, 'DD-MM-YYYY') as dob,
        TO_CHAR(update_by, 'DD-MM-YYYY HH24:MI') as update_by
      FROM filtered_voters
      ORDER BY vsno ASC
      LIMIT $${params.length + 1}
      OFFSET $${params.length + 2}
    `;

      const voterParams = [...params, limit, offset];
      const { rows: votersRaw } = await pool.query(voterSql, voterParams);

      const countSql = `
      ${baseQuery}
      SELECT COUNT(*) FROM filtered_voters
    `;

      const countRes = await pool.query(countSql, params);
      const total = Number(countRes.rows[0].count);

      const mappingSql = `
      ${baseQuery}
      SELECT DISTINCT
        village_id, village,
        gp_ward_id, gp_ward,
        block_id, block,
        kendra_id, kendra,
        mandal_id, mandal,
        pincode_id, pincode,
        postoff_id, postoff,
        policst_id, policst,
        bhag_no,
        sec_no,
        mapping_section AS section,
        ru,
        castid,
        sex
      FROM filtered_voters
    `;

      const mappingRes = await pool.query(mappingSql, params);
      const rows = mappingRes.rows;

      const transformedMapping = {};

      const buildPairArray = (idKey, nameKey) => {
        const map = new Map();

        rows.forEach((row) => {
          if (row[idKey] != null && row[nameKey] != null) {
            map.set(row[idKey], {
              [idKey]: row[idKey],
              [nameKey]: row[nameKey]
            });
          }
        });

        return Array.from(map.values()).sort((a, b) =>
          String(a[nameKey]).localeCompare(String(b[nameKey]))
        );
      };

      if (rows.length > 0) {
        transformedMapping.village = buildPairArray("village_id", "village");
        transformedMapping.gp_ward = buildPairArray("gp_ward_id", "gp_ward");
        transformedMapping.block = buildPairArray("block_id", "block");
        transformedMapping.kendra = buildPairArray("kendra_id", "kendra");
        transformedMapping.mandal = buildPairArray("mandal_id", "mandal");
        transformedMapping.pincode = buildPairArray("pincode_id", "pincode");
        transformedMapping.postoff = buildPairArray("postoff_id", "postoff");
        transformedMapping.policst = buildPairArray("policst_id", "policst");

        transformedMapping.bhag_no = [
          ...new Set(rows.map((r) => r.bhag_no).filter((v) => v != null))
        ].sort((a, b) => a - b);

        transformedMapping.ru = [
          ...new Set(rows.map((r) => r.ru).filter((v) => v != null))
        ].sort((a, b) => a - b);

        const secMap = new Map();
        rows.forEach((row) => {
          if (row.sec_no != null) {
            secMap.set(row.sec_no, {
              sec_no: row.sec_no,
              section:
                row.section && String(row.section).trim() !== ""
                  ? row.section
                  : String(row.sec_no)
            });
          }
        });

        transformedMapping.section = Array.from(secMap.values()).sort(
          (a, b) => Number(a.sec_no) - Number(b.sec_no)
        );

        transformedMapping.castid = [
          ...new Set(
            rows
              .map((r) => (r.castid != null ? String(r.castid).trim().toUpperCase() : null))
              .filter((v) => v != null && v !== "")
          )
        ].sort((a, b) => String(a).localeCompare(String(b)));

        transformedMapping.sex = [
          ...new Set(rows.map((r) => r.sex).filter((v) => v != null && String(v).trim() !== ""))
        ].sort((a, b) => String(a).localeCompare(String(b)));
      }

      let voters = votersRaw;
      let visible_columns = votersRaw.length ? Object.keys(votersRaw[0]) : [];

      if (!isSuperUser) {
        const preparedAssignments = prepareAssignments(dataAssignments);
        const permissionsByAssignment = buildPermissionMapByAssignment(rawColumnPermissions);

        const processedVoters = [];
        const visibleColumnSet = new Set();

        for (const voter of votersRaw) {
          const matchedAssignments = preparedAssignments.filter((assignment) =>
            doesRowMatchAssignment(voter, assignment)
          );

          if (!matchedAssignments.length) continue;

          const mergedPermissions = getMergedPermissionsForAssignments(
            matchedAssignments,
            permissionsByAssignment
          );

          if (!mergedPermissions || !mergedPermissions.length) continue;

          const compiled = compilePermissions(mergedPermissions);
          const finalRow = applyCompiledPermissionsToSingleRow(voter, compiled);

          compiled.visibleColumns.forEach((col) => visibleColumnSet.add(col));
          processedVoters.push(finalRow);
        }

        voters = processedVoters;
        visible_columns = Array.from(visibleColumnSet);
      }

      return {
        voters,
        total,
        mapping: transformedMapping,
        visible_columns
      };
    } catch (error) {
      console.error("applyAdvancedFilter error:", error);
      throw error;
    }
  },

  async getCastIdLookup({ search = "", data_id = null, limit = 20 }) {
    try {
      const values = [];
      let idx = 1;
      const where = [];

      const searchText = String(search || "").trim().toUpperCase();

      if (!searchText) {
        return [];
      }

      if (data_id !== null && data_id !== undefined && data_id !== "") {
        const dataIds = getArrayFilter(data_id)
          .map((v) => Number(v))
          .filter((v) => Number.isFinite(v));

        if (dataIds.length) {
          where.push(`ecm.data_id = ANY($${idx}::int[])`);
          values.push(dataIds);
          idx++;
        }
      }

      // only exact castid match
      where.push(`UPPER(TRIM(COALESCE(ecm.castid, ''))) = $${idx}`);
      values.push(searchText);
      idx++;

      // only meaningful castid rows
      where.push(`NULLIF(TRIM(COALESCE(ecm.castid, '')), '') IS NOT NULL`);

      const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

      const sql = `
      SELECT DISTINCT ON (UPPER(TRIM(COALESCE(ecm.castid, ''))), ecm.data_id)
        TRIM(COALESCE(ecm.castid, '')) AS castid,
        COALESCE(NULLIF(TRIM(ecm.castida_hi), ''), '') AS caste,
        COALESCE(NULLIF(TRIM(ecm.catid), ''), '') AS cast_cat,
        COALESCE(NULLIF(TRIM(ecm.religion_hi), ''), NULLIF(TRIM(ecm.religion_en), ''), '') AS religion,
        ecm.data_id
      FROM eroll_castmaster ecm
      ${whereSql}
      ORDER BY
        UPPER(TRIM(COALESCE(ecm.castid, ''))),
        ecm.data_id,
        ecm.id DESC
      LIMIT $${idx}
    `;

      values.push(Number(limit) || 1);

      const result = await pool.query(sql, values);
      return result.rows || [];
    } catch (error) {
      console.error("getCastIdLookup error:", error);
      throw error;
    }
  },

  async getCastSurnameLookup({ search = "", data_id = null, limit = 20 }) {
    try {
      const values = [];
      let idx = 1;
      const where = [];

      const searchText = String(search || "").trim();
      const normalizedSearch = searchText.replace(/\s+/g, " ").trim();
      const upperSearch = normalizedSearch.toUpperCase();
      const regexValue = normalizedSearch ? buildLooseSearchRegex(normalizedSearch) : "";

      if (data_id !== null && data_id !== undefined && data_id !== "") {
        const dataIds = getArrayFilter(data_id)
          .map((v) => Number(v))
          .filter((v) => Number.isFinite(v));

        if (dataIds.length) {
          where.push(`base.data_id = ANY($${idx}::int[])`);
          values.push(dataIds);
          idx++;
        }
      }

      // only meaningful castid_surname rows
      where.push(`NULLIF(TRIM(COALESCE(base.castid_surname, '')), '') IS NOT NULL`);

      if (normalizedSearch) {
        where.push(`(
        UPPER(TRIM(COALESCE(base.castid_surname, ''))) LIKE UPPER($${idx})
        OR UPPER(TRIM(COALESCE(base.surname, ''))) LIKE UPPER($${idx})
        OR UPPER(TRIM(COALESCE(base.subcast, ''))) LIKE UPPER($${idx})

        OR TRIM(COALESCE(base.castid_surname, '')) ~* $${idx + 1}
        OR TRIM(COALESCE(base.surname, '')) ~* $${idx + 1}
        OR TRIM(COALESCE(base.subcast, '')) ~* $${idx + 1}
      )`);

        values.push(`%${normalizedSearch}%`);
        values.push(regexValue);
        idx += 2;
      }

      const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

      const sql = `
      WITH base AS (
        SELECT
          ed.id,
          ed.data_id,
          TRIM(COALESCE(ed.castid_surname, '')) AS castid_surname,
          TRIM(
            CASE
              WHEN jsonb_typeof(ed.surname) = 'object' THEN COALESCE(ed.surname ->> 'v', '')
              ELSE ''
            END
          ) AS surname,
          TRIM(COALESCE(ed.subcast, '')) AS subcast,
          COALESCE(ed.oldnew, 0) AS oldnew
        FROM eroll_db ed
      ),
      ranked AS (
        SELECT
          base.*,
          CASE
            WHEN $${idx} <> '' AND UPPER(base.castid_surname) = $${idx} THEN 1
            WHEN $${idx} <> '' AND UPPER(base.surname) = $${idx} THEN 2
            WHEN $${idx} <> '' AND UPPER(base.subcast) = $${idx} THEN 3

            WHEN $${idx + 1} <> '' AND UPPER(base.castid_surname) LIKE $${idx + 1} THEN 4
            WHEN $${idx + 1} <> '' AND UPPER(base.surname) LIKE $${idx + 1} THEN 5
            WHEN $${idx + 1} <> '' AND UPPER(base.subcast) LIKE $${idx + 1} THEN 6

            WHEN $${idx + 2} <> '' AND base.castid_surname ~* $${idx + 2} THEN 7
            WHEN $${idx + 2} <> '' AND base.surname ~* $${idx + 2} THEN 8
            WHEN $${idx + 2} <> '' AND base.subcast ~* $${idx + 2} THEN 9

            ELSE 99
          END AS search_rank
        FROM base
        ${whereSql}
      )
      SELECT DISTINCT ON (
        UPPER(castid_surname),
        UPPER(surname),
        COALESCE(data_id, 0)
      )
        castid_surname,
        surname,
        subcast,
        oldnew,
        data_id
      FROM ranked
      ORDER BY
        UPPER(castid_surname),
        UPPER(surname),
        COALESCE(data_id, 0),
        search_rank ASC,
        id DESC
      LIMIT $${idx + 3}
    `;

      values.push(upperSearch);
      values.push(upperSearch ? `${upperSearch}%` : "");
      values.push(regexValue || "");
      values.push(Number(limit) || 20);

      const result = await pool.query(sql, values);
      return result.rows || [];
    } catch (error) {
      console.error("getCastSurnameLookup error:", error);
      throw error;
    }
  },

  bulkUpdateVoters: async (updates) => {
    const client = await pool.connect();

    const mappingKeys = new Set([
      "block_id",
      "gp_ward_id",
      "village_id",
      "kendra_id",
      "mandal_id",
      "pjila_id",
      "pincode_id",
      "policst_id",
      "postoff_id",
      "psb_id",
      "coordinate_id",
    ]);

    const erollDbColumns = new Set([
      "card_id",
      "ac_no",
      "bhag_no",
      "sec_no",
      "section",
      "epic",
      "vsno",
      "vname",
      "sex",
      "age",
      "dob",
      "relation",
      "rname",
      "hno",
      "phone1",
      "phone2",
      "castid",
      "cast_cat",
      "castid_surname",
      "subcast",
      "oldnew",
      "familyid",
      "hof",
      "hof_relation",
      "surname",
      "star",
      "photo",
      "pdob_verify",
      "msg",
      "voter_view",
      "v_parchi",
      "worker_id",
      "edu_id",
      "proff_id",
      "proff_city",
      "bank_name",
      "acc_no",
      "ifsc",
      "upi_id",
      "aadhar_no",
    ]);

    try {
      await client.query("BEGIN");

      for (const voter of updates) {
        const { id, data_id, ...fields } = voter;

        if (id == null || data_id == null) {
          throw new Error("id and data_id are required");
        }

        const directFields = {};
        const mappingFields = {};

        for (const [key, value] of Object.entries(fields)) {
          if (value === undefined) continue;

          if (mappingKeys.has(key)) {
            mappingFields[key] = value;
            continue;
          }

          if (erollDbColumns.has(key)) {
            directFields[key] = value;
            continue;
          }
        }

        console.log("directFields ->", directFields);
        console.log("mappingFields ->", mappingFields);

        const setParts = [];
        const values = [];
        let paramIndex = 1;

        for (const [key, value] of Object.entries(directFields)) {
          setParts.push(`"${key}" = $${paramIndex}`);
          values.push(value);
          paramIndex++;
        }

        if (Object.keys(mappingFields).length > 0) {
          setParts.push(
            `mapping = COALESCE(mapping, '{}'::jsonb) || $${paramIndex}::jsonb`
          );
          values.push(JSON.stringify(mappingFields));
          paramIndex++;
        }

        if (setParts.length === 0) {
          console.log(`Skipped row id=${id}, data_id=${data_id} because nothing to update`);
          continue;
        }

        setParts.push(`update_by = CURRENT_TIMESTAMP`);

        values.push(id, data_id);

        const checkRow = await client.query(
          `
        SELECT id, data_id, castid, cast_cat, mapping, update_by
        FROM eroll_db
        WHERE id = $1 AND data_id = $2
        `,
          [id, data_id]
        );

        console.log("before update rowCount ->", checkRow.rowCount);
        console.log("before update row ->", checkRow.rows[0] || null);

        const updateQuery = `
        UPDATE eroll_db
        SET ${setParts.join(", ")}
        WHERE id = $${paramIndex}
          AND data_id = $${paramIndex + 1}
        RETURNING id, data_id, castid, cast_cat, mapping, update_by
      `;

        console.log("updateQuery ->", updateQuery);
        console.log("values ->", values);

        const updateResult = await client.query(updateQuery, values);

        console.log("updateResult.rowCount ->", updateResult.rowCount);
        console.log("updated row ->", updateResult.rows[0] || null);

        if (updateResult.rowCount === 0) {
          throw new Error(
            `No row updated for id=${id} and data_id=${data_id}. Check whether the row exists in eroll_db.`
          );
        }
      }

      await client.query("COMMIT");
      return true;
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("bulkUpdateVoters error ->", error);
      throw error;
    } finally {
      client.release();
    }
  },

  // register print
  getDataByDataIdWiseType: async (data_id, wise_type) => {
    try {
      if (!data_id || !wise_type) {
        throw new Error("data_id and wise_type are required");
      }

      let selectFields = "";

      switch (wise_type) {
        case "bhag":
          selectFields = `
          m.bhag_no AS id_no,
          m.bhag AS name
        `;
          break;

        case "section":
          selectFields = `
          m.sec_no AS id_no,
          m.section AS name
        `;
          break;

        case "village":
          selectFields = `
          m.village_id AS id_no,
          m.village AS name
        `;
          break;

        case "gp_ward":
          selectFields = `
          m.gp_ward_id AS id_no,
          m.gp_ward AS name
        `;
          break;

        case "block":
          selectFields = `
          m.block_id AS id_no,
          m.block AS name
        `;
          break;

        default:
          throw new Error("Invalid wise_type");
      }

      const query = `
      WITH wise_data AS (
        SELECT DISTINCT
          ${selectFields}
        FROM eroll_db e
        JOIN eroll_mapping m
          ON e.ac_no = m.ac_id
          AND e.bhag_no = m.bhag_no
          AND e.sec_no = m.sec_no
        WHERE e.data_id = $1
      ),
      cast_data AS (
        SELECT DISTINCT castId
        FROM eroll_db
        WHERE data_id = $1
        AND castId IS NOT NULL
      )

      SELECT
        (SELECT json_agg(wise_data ORDER BY id_no) FROM wise_data) AS wise,
        (SELECT array_agg(castId ORDER BY castId) FROM cast_data) AS castids
    `;

      const result = await pool.query(query, [data_id]);

      return result.rows[0];

    } catch (error) {
      console.error("Error fetching wise data:", error);
      throw error;
    }
  },

  getFilteredData: async (filters) => {
    try {
      let query = `
      SELECT 
        e.data_id,
        e.bhag_no,
        e.familyid,
        e.hno,
        e.vsno,
        e.vname,
        e.rname,
        e.relation,
        e.section,
        e.age,
        e.sex,
        e.dob,
        e.edu_id,
        e.ac_no,
        e.phone1,
        e.cast_cat,
        e.castid,
        e.familyid,
        m.village,
        m.gp_ward,
        m.block
      FROM eroll_db e
      LEFT JOIN eroll_mapping m
        ON e.data_id = m.data_id
        AND e.ac_no = m.ac_id
        AND e.bhag_no = m.bhag_no
      WHERE e.data_id = $1
    `;

      const values = [filters.data_id];
      let index = 2;

      // Add bhag filter if provided
      if (filters.bhag && Array.isArray(filters.bhag) && filters.bhag.length > 0) {
        // Convert to integers if they're strings
        const bhagNumbers = filters.bhag.map(b => {
          if (typeof b === 'string') return parseInt(b, 10);
          return b;
        });

        query += ` AND e.bhag_no = ANY($${index++}::int[])`;
        values.push(bhagNumbers);
      }

      // Add sorting
      query += `
      ORDER BY 
        e.bhag_no ASC,
        e.vsno ASC
    `;

      const result = await pool.query(query, values);

      if (result.rows.length > 0) {
        const vsnos = result.rows.map(r => r.vsno);
      }

      return {
        erollData: result.rows
      };

    } catch (error) {
      console.error("Database Query Error:", error);
      throw error;
    }
  },

  deleteDynamic: async (table, ids) => {

    const allowedTables = ["dataid_importmaster"];
    if (!allowedTables.includes(table)) {
      throw new Error("Invalid table name");
    }

    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      let resultSummary = {};

      if (table === "dataid_importmaster") {
        const mappingDelete = await client.query(
          `DELETE FROM eroll_mapping 
         WHERE data_id = ANY($1)
         RETURNING data_id`,
          [ids]
        );

        const dbDelete = await client.query(
          `DELETE FROM eroll_db 
         WHERE data_id = ANY($1)
         RETURNING data_id`,
          [ids]
        );

        const masterDelete = await client.query(
          `DELETE FROM dataid_importmaster 
         WHERE data_id = ANY($1)
         RETURNING data_id`,
          [ids]
        );

        resultSummary = {
          master_deleted: masterDelete.rowCount,
          mapping_deleted: mappingDelete.rowCount,
          db_deleted: dbDelete.rowCount
        };

      }

      await client.query("COMMIT");

      try {
        await refreshGlobalCache();
      } catch (cacheError) {
        console.error("Failed to refresh cache:", cacheError);
      }

      return resultSummary;

    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  },

  // master tables
  getDynamicTable: async (queryParams) => {

    const { table, data_id, status, dropdown_name, reg_name, search } = queryParams;

    const allowedTables = Object.keys(tableConfig);

    if (!allowedTables.includes(table)) {
      throw new Error("Invalid table name");
    }

    const config = tableConfig[table];

    let conditions = [];
    let values = [];
    let index = 1;

    // DATA_ID FILTER
    if (data_id && config.filters.includes("data_id")) {
      conditions.push(`data_id = $${index}`);
      values.push(data_id);
      index++;
    }

    // STATUS FILTER
    if (status !== undefined && config.filters.includes("is_active")) {
      conditions.push(`is_active = $${index}`);
      values.push(status);
      index++;
    }

    // DROPDOWN FILTER
    if (dropdown_name && config.filters.includes("dropdown_name")) {
      conditions.push(`dropdown_name = $${index}`);
      values.push(dropdown_name);
      index++;
    }

    // REG_NAME FILTER
    if (reg_name && config.filters.includes("reg_name")) {
      conditions.push(`reg_name = $${index}`);
      values.push(reg_name);
      index++;
    }

    // SEARCH FILTER
    // SEARCH FILTER ON ALL TABLE COLUMNS
    if (search) {
      const columnSql = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = $1
    ORDER BY ordinal_position
  `;

      const columnResult = await pool.query(columnSql, [table]);

      const searchableColumns = columnResult.rows
        .map((row) => row.column_name)
        .filter((col) => !["created_at", "updated_at"].includes(col)); // optional exclude

      if (searchableColumns.length > 0) {
        const searchConditions = searchableColumns.map(
          (col) => `COALESCE(${col}::text, '') ~* $${index}`
        );

        conditions.push(`(${searchConditions.join(" OR ")})`);
        values.push(search);
        index++;
      }
    }

    // DATA QUERY
    let dataQuery = `SELECT * FROM ${table}`;

    if (conditions.length > 0) {
      dataQuery += ` WHERE ${conditions.join(" AND ")}`;
    }

    if (config.orderBy) {
      dataQuery += ` ORDER BY ${config.orderBy}`;
    }

    const dataResult = await pool.query(dataQuery, values);

    // FILTER QUERY
    const filterColumns = config.filters
      .map(col => `ARRAY_AGG(DISTINCT ${col}) AS ${col}`)
      .join(",");

    let filters = {};

    if (filterColumns) {

      const filterQuery = `
      SELECT ${filterColumns}
      FROM ${table}
    `;

      const filterResult = await pool.query(filterQuery);
      filters = filterResult.rows[0];
    }

    const dataidSql = `
      SELECT data_id, data_id_name_hi, data_id_name_en
      FROM dataid_importmaster
      WHERE is_active = 1
      ORDER BY data_id ASC
    `;
    const dataidResult = await pool.query(dataidSql, [])

    return {
      result: dataResult.rows,
      filters,
      dataidRows: dataidResult?.rows
    };

  },

  getExistingMasterRecord: async (table, data) => {
    try {
      if (!table || !insertTableConfig[table]) {
        throw new Error("Invalid table name");
      }

      let query = "";
      let values = [];

      if (table === "dataid_importmaster") {
        query = `SELECT id, data_id FROM ${table} WHERE data_id = $1 LIMIT 1`;
        values = [data.data_id];
      } else if (table === "eroll_castmaster") {
        query = `
        SELECT id, data_id, castid
        FROM ${table}
        WHERE data_id = $1 AND castid = $2
        LIMIT 1
      `;
        values = [data.data_id, data.castid || null];
      } else if (table === "eroll_dropdown") {
        query = `
        SELECT id, data_id, dropdown_name, value_id
        FROM ${table}
        WHERE data_id = $1 AND dropdown_name = $2 AND value_id = $3
        LIMIT 1
      `;
        values = [data.data_id, data.dropdown_name || null, data.value_id || null];
      } else if (table === "eroll_yojna_master") {
        query = `
        SELECT id, data_id, yojna_id
        FROM ${table}
        WHERE data_id = $1 AND yojna_id = $2
        LIMIT 1
      `;
        values = [data.data_id, data.yojna_id || null];
      }

      if (!query) return null;

      const result = await pool.query(query, values);
      return result.rows[0] || null;
    } catch (error) {
      console.error("getExistingMasterRecord error:", error);
      throw error;
    }
  },

  insertMaster: async (data) => {
    try {
      const { table } = data;

      if (!table || !insertTableConfig[table]) {
        throw new Error("Invalid table name");
      }

      const config = insertTableConfig[table];

      for (const field of config.required) {
        if (
          data[field] === undefined ||
          data[field] === null ||
          data[field] === ""
        ) {
          throw new Error(`${field} is required`);
        }
      }

      const columns = [];
      const placeholders = [];
      const values = [];
      let index = 1;

      for (const column of config.columns) {
        let value = data[column];

        if ((value === undefined || value === "") && config.defaults[column] !== undefined) {
          value = config.defaults[column];
        }

        if (column === "updated_at") {
          value = new Date();
        }

        if (column === "data_range" && value) {
          value = typeof value === "object" ? JSON.stringify(value) : value;
        }

        if (value === undefined || value === "") {
          value = null;
        }

        columns.push(column);
        placeholders.push(`$${index}`);
        values.push(value);
        index++;
      }

      const query = `
      INSERT INTO ${table} (${columns.join(", ")})
      VALUES (${placeholders.join(", ")})
      RETURNING *;
    `;

      const result = await pool.query(query, values);

      try {
        await refreshGlobalCache();
      } catch (cacheError) {
        console.error("Cache refresh failed:", cacheError);
      }

      return result.rows[0];
    } catch (error) {
      console.error(`Error inserting into ${data.table}:`, error);
      throw error;
    }
  },

  deleteMasterRow: async (data) => {
    try {
      const { table, id, data_id } = data;

      if (!table || !deleteTableConfig[table]) {
        return {
          success: false,
          message: "Invalid table name"
        };
      }

      if (!id || isNaN(Number(id))) {
        return {
          success: false,
          message: "Valid id is required"
        };
      }

      if (data_id === undefined || data_id === null || data_id === "") {
        return {
          success: false,
          message: "data_id is required"
        };
      }

      const query = `
      DELETE FROM ${table}
      WHERE id = $1 AND data_id = $2
      RETURNING *;
    `;

      const values = [Number(id), data_id];
      const result = await pool.query(query, values);

      if (result.rowCount === 0) {
        return {
          success: false,
          message: "No matching row found"
        };
      }

      try {
        await refreshGlobalCache();
      } catch (cacheError) {
        console.error("Cache refresh failed:", cacheError);
      }

      return {
        success: true,
        message: "Row deleted successfully",
        data: result.rows[0]
      };
    } catch (error) {
      console.error("deleteMasterRow error:", error);
      return {
        success: false,
        message: error.message || "Something went wrong"
      };
    }
  },

  getMasterByDataId: async (data_id) => {
    try {
      const query = `
            SELECT id, data_id
            FROM dataid_importmaster
            WHERE data_id = $1
            LIMIT 1
        `;

      const result = await pool.query(query, [data_id]);

      return result.rows[0] || null;

    } catch (error) {
      console.error("Error checking data_id:", error);
      throw error;
    }
  },

  deleteDataIds: async (data_ids) => {

    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      // 1️⃣ Delete from eroll_mapping
      const mappingDelete = await client.query(
        `DELETE FROM eroll_mapping
       WHERE data_id = ANY($1)
       RETURNING data_id`,
        [data_ids]
      );

      // 2️⃣ Delete from eroll_db
      const dbDelete = await client.query(
        `DELETE FROM eroll_db
       WHERE data_id = ANY($1)
       RETURNING data_id`,
        [data_ids]
      );

      // 3️⃣ Delete from master table
      const masterDelete = await client.query(
        `DELETE FROM dataid_importmaster
       WHERE data_id = ANY($1)
       RETURNING data_id`,
        [data_ids]
      );

      await client.query("COMMIT");

      return {
        master_deleted: masterDelete.rowCount,
        mapping_deleted: mappingDelete.rowCount,
        db_deleted: dbDelete.rowCount
      };

    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  },

  // mapping tables
  getErollMappingDynamic: async (queryParams) => {
    const {
      data_id,
      district,
      district_id,
      ac_id,
      ac_no,
      pc_id,
      pc_no,

      bhag_no,
      sec_no,
      village_id,
      gp_ward_id,
      psb_id,
      pincode_id,
      postoff_id,
      policst_id,
      block_id,
      mandal_id,
      kendra_id,

      initial_load = false,
      page = 1,
      limit = 50
    } = queryParams;

    let conditions = [];
    let values = [];
    let index = 1;

    if (data_id) {
      conditions.push(`dm.data_id = $${index}`);
      values.push(data_id);
      index++;
    }

    if (district_id) {
      conditions.push(`dm.district_id = $${index}`);
      values.push(district_id);
      index++;
    }

    if (ac_id || ac_no) {
      conditions.push(`dm.ac_no = $${index}`);
      values.push(ac_id || ac_no);
      index++;
    }

    if (pc_id || pc_no) {
      conditions.push(`dm.pc_no = $${index}`);
      values.push(pc_id || pc_no);
      index++;
    }

    if (!initial_load) {

      if (bhag_no) {
        conditions.push(`em.bhag_no = $${index}`);
        values.push(bhag_no);
        index++;
      }

      if (sec_no) {
        conditions.push(`em.sec_no = $${index}`);
        values.push(sec_no);
        index++;
      }

      if (village_id) {
        conditions.push(`em.village_id = $${index}`);
        values.push(village_id);
        index++;
      }

      if (gp_ward_id) {
        conditions.push(`em.gp_ward_id = $${index}`);
        values.push(gp_ward_id);
        index++;
      }

      if (psb_id) {
        conditions.push(`em.psb_id = $${index}`);
        values.push(psb_id);
        index++;
      }

      if (pincode_id) {
        conditions.push(`em.pincode_id = $${index}`);
        values.push(pincode_id);
        index++;
      }

      if (postoff_id) {
        conditions.push(`em.postoff_id = $${index}`);
        values.push(postoff_id);
        index++;
      }

      if (policst_id) {
        conditions.push(`em.policst_id = $${index}`);
        values.push(policst_id);
        index++;
      }

      if (block_id) {
        conditions.push(`em.block_id = $${index}`);
        values.push(block_id);
        index++;
      }

      if (mandal_id) {
        conditions.push(`em.mandal_id = $${index}`);
        values.push(mandal_id);
        index++;
      }

      if (kendra_id) {
        conditions.push(`em.kendra_id = $${index}`);
        values.push(kendra_id);
        index++;
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const offset = (page - 1) * limit;

    const dataQuery = `
    SELECT DISTINCT ON (em.data_id, em.ac_id, em.bhag_no, em.sec_no)
      em.id,
      em.data_id,
      em.ac_id,
      em.bhag_no,
      em.bhag,
      em.sec_no,
      em.section,
      em.ru,
      em.village,
      em.village_id,
      em.gp_ward,
      em.gp_ward_id,
      em.block,
      em.block_id,
      em.mandal,
      em.mandal_id,
      em.kendra,
      em.kendra_id,
      em.psb,
      em.psb_id,
      em.coordinate,
      em.coordinate_id,
      em.pjila,
      em.pjila_id,
      em.pincode,
      em.pincode_id,
      em.postoff,
      em.postoff_id,
      em.policst,
      em.policst_id
    FROM eroll_mapping em
    JOIN dataid_importmaster dm
      ON em.data_id = dm.data_id
    ${whereClause}
    ORDER BY 
      em.data_id,
      em.ac_id,
      em.bhag_no,
      em.sec_no,
      em.updated_at DESC NULLS LAST
    LIMIT $${index} OFFSET $${index + 1}
  `;

    const dataValues = [...values, limit, offset];
    const dataResult = await pool.query(dataQuery, dataValues);

    const countQuery = `
SELECT COUNT(DISTINCT (em.data_id, em.ac_id, em.bhag_no, em.sec_no))
FROM eroll_mapping em
JOIN dataid_importmaster dm
ON em.data_id = dm.data_id
${whereClause}
  `;

    const countResult = await pool.query(countQuery, values);

    let uniqueMapping = null;

    if (initial_load === true || initial_load === "true") {

      const filterQuery = `
      SELECT
      JSONB_AGG(
        DISTINCT jsonb_build_object(
        'bhag_no', em.bhag_no,
        'bhag', COALESCE(em.bhag, 'NULL')
        )
      ) FILTER (WHERE em.bhag_no IS NOT NULL) AS bhag,


      JSONB_AGG(
        DISTINCT jsonb_build_object(
        'sec_no', em.sec_no,
        'section', COALESCE(em.section, '???')
        )
      ) FILTER (WHERE em.sec_no IS NOT NULL) AS section,

        JSONB_AGG(DISTINCT jsonb_build_object(
          'village_id', em.village_id,
          'village', em.village
        )) FILTER (WHERE em.village IS NOT NULL) AS village,

        JSONB_AGG(DISTINCT jsonb_build_object(
          'gp_ward_id', em.gp_ward_id,
          'gp_ward', em.gp_ward
        )) FILTER (WHERE em.gp_ward IS NOT NULL) AS gp_ward,

        JSONB_AGG(DISTINCT jsonb_build_object(
          'psb_id', em.psb_id,
          'psb', em.psb
        )) FILTER (WHERE em.psb IS NOT NULL) AS psb,

        JSONB_AGG(DISTINCT jsonb_build_object(
          'pincode_id', em.pincode_id,
          'pincode', em.pincode
        )) FILTER (WHERE em.pincode IS NOT NULL) AS pincode,

        JSONB_AGG(DISTINCT jsonb_build_object(
          'postoff_id', em.postoff_id,
          'postoff', em.postoff
        )) FILTER (WHERE em.postoff IS NOT NULL) AS postoff,

        JSONB_AGG(DISTINCT jsonb_build_object(
          'policst_id', em.policst_id,
          'policst', em.policst
        )) FILTER (WHERE em.policst IS NOT NULL) AS policst,

      JSONB_AGG(
        DISTINCT jsonb_build_object(
          'ru_id', em.ru,
          'ru',
          CASE
            WHEN em.ru = 0 THEN 'Urban'
            WHEN em.ru = 1 THEN 'Rural'
            ELSE 'Unknown'
          END
        )
      ) FILTER (WHERE em.ru IS NOT NULL) AS ru

      FROM eroll_mapping em
      JOIN dataid_importmaster dm
        ON em.data_id = dm.data_id
      ${whereClause}
    `;

      const filterResult = await pool.query(filterQuery, values);
      uniqueMapping = filterResult.rows[0];
    }

    return {
      table_name: "eroll_mapping",
      result: dataResult.rows,
      unique_mapping: uniqueMapping,
      total: Number(countResult.rows[0].count),
      page: Number(page),
      limit: Number(limit)
    };
  },

  addDynamicMasterRow: async (body) => {
    const { table, values } = body;

    const insertConfig = {
      eroll_castmaster: {
        allowedColumns: [
          'rid',
          'religion_en',
          'religion_hi',
          'catid',
          'castcat_en',
          'castcat_hi',
          'castid',
          'castida_en',
          'castida_hi',
          'data_id'
        ],
        requiredColumns: []
      },

      eroll_dropdown: {
        allowedColumns: [
          'dropdown_id',
          'dropdown_name',
          'value_hi',
          'value_en',
          'data_id',
          'value_id'
        ],
        requiredColumns: []
      },

      eroll_yojna_master: {
        allowedColumns: [
          'yojna_name',
          'regid',
          'reg_name',
          'data_id',
          'is_active',
          'updated_at',
          'yojna_id'
        ],
        requiredColumns: []
      }
    };

    if (!table || !insertConfig[table]) {
      throw new Error('Invalid table name');
    }

    if (!values || typeof values !== 'object' || Array.isArray(values)) {
      throw new Error('values must be an object');
    }

    const config = insertConfig[table];
    const allowedColumns = config.allowedColumns;

    const incomingColumns = Object.keys(values);

    if (incomingColumns.length === 0) {
      throw new Error('No values provided for insert');
    }

    const invalidColumns = incomingColumns.filter(
      (col) => !allowedColumns.includes(col)
    );

    if (invalidColumns.length > 0) {
      throw new Error(`Invalid columns for ${table}: ${invalidColumns.join(', ')}`);
    }

    for (const requiredCol of config.requiredColumns) {
      if (
        values[requiredCol] === undefined ||
        values[requiredCol] === null ||
        values[requiredCol] === ''
      ) {
        throw new Error(`${requiredCol} is required`);
      }
    }

    const columns = [];
    const placeholders = [];
    const queryValues = [];
    let index = 1;

    for (const col of incomingColumns) {
      columns.push(col);
      placeholders.push(`$${index}`);
      queryValues.push(values[col]);
      index++;
    }

    const insertQuery = `
    INSERT INTO ${table} (${columns.join(', ')})
    VALUES (${placeholders.join(', ')})
    RETURNING *
  `;

    const result = await pool.query(insertQuery, queryValues);

    return result.rows[0];
  },

  processMappingFile: async (filePath, dataId) => {
    const client = await pool.connect();

    const { v4: uuidv4 } = require('uuid');

    // Generate TBO ID with readable format
    const generateTboId = (type, parts = []) => {
      const uuid = uuidv4().replace(/-/g, '');

      switch (type) {
        case 'BLK': // Block
          return `TBO-BLK-${uuid.substring(0, 6)}-${uuid.substring(6, 10)}`;

        case 'GPW': // GP Ward (depends on block)
          return `TBO-GPW-${parts[0]}-${uuid.substring(0, 6)}`;

        case 'VIL': // Village (depends on block & gp ward)
          return `TBO-VIL-${parts[0]}-${parts[1]}-${uuid.substring(0, 4)}`;

        case 'PSB': // PSB (independent)
          return `TBO-PSB-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'CRD': // Coordinate (independent)
          return `TBO-CRD-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'KEN': // Kendra (independent)
          return `TBO-KEN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'MAN': // Mandal (independent)
          return `TBO-MAN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'PJI': // Pjila (independent)
          return `TBO-PJI-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'PIN': // Pincode (independent)
          return `TBO-PIN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'POS': // Postoff (independent)
          return `TBO-POS-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'POL': // Policst (independent)
          return `TBO-POL-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        default:
          return `TBO-${uuid.substring(0, 12)}`;
      }
    };

    // Independent fields (don't depend on anything)
    const independentFields = [
      "psb",
      "coordinate",
      "kendra",
      "mandal",
      "pjila",
      "pincode",
      "postoff",
      "policst"
    ];

    // Caches to maintain consistency across rows
    const idCache = {
      block: new Map(),        // blockName -> blockId
      gpWard: new Map(),       // blockId_gpWardName -> gpWardId
      village: new Map(),      // gpWardId_villageName -> villageId
      psb: new Map(),          // psbName -> psbId
      coordinate: new Map(),   // coordinateName -> coordinateId
      kendra: new Map(),       // kendraName -> kendraId
      mandal: new Map(),       // mandalName -> mandalId
      pjila: new Map(),        // pjilaName -> pjilaId
      pincode: new Map(),      // pincodeValue -> pincodeId
      postoff: new Map(),      // postoffName -> postoffId
      policst: new Map()       // policstName -> policstId
    };

    try {
      await client.query('BEGIN');

      // Read Excel file
      const workbook = XLSX.readFile(filePath);
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];

      // Convert to JSON
      const rows = XLSX.utils.sheet_to_json(worksheet, { header: 1 });

      if (rows.length < 2) {
        throw new Error("File has no data rows");
      }

      const headers = rows[0];
      const dataRows = rows.slice(1);

      // Define mapping columns (from your table structure)
      const mappingColumns = [
        "data_id", "ac_id", "ac_name", "bhag_no", "bhag", "sec_no",
        "section", "ru", "village", "village_id", "gp_ward", "gp_ward_id",
        "block", "block_id", "psb", "psb_id", "coordinate", "coordinate_id",
        "kendra", "kendra_id", "mandal", "mandal_id", "pjila", "pjila_id",
        "pincode", "pincode_id", "postoff", "postoff_id", "policst", "policst_id",
      ];

      // Special fields for mapping JSON
      const specialFields = [
        "village_id", "gp_ward_id", "block_id", "psb_id", "coordinate_id",
        "kendra_id", "mandal_id", "pjila_id", "pincode_id", "postoff_id", "policst_id"
      ];

      await client.query(
        "DELETE FROM eroll_mapping WHERE data_id = $1",
        [dataId]
      );

      let insertedCount = 0;
      let updatedErollCount = 0;

      const uniqueBlocks = new Set();
      const uniqueGpWards = new Map();
      const uniqueVillages = new Map();

      for (const row of dataRows) {
        if (!row || row.length === 0) continue;

        const rowData = {};
        headers.forEach((header, index) => {
          if (header && row[index] !== undefined) {
            rowData[header] = row[index];
          }
        });

        if (Object.keys(rowData).length === 0) continue;

        if (rowData.block) {
          uniqueBlocks.add(rowData.block);
        }
      }

      // Generate block IDs
      for (const blockName of uniqueBlocks) {
        if (!idCache.block.has(blockName)) {
          // Check if block_id already exists in DB for this block name
          const blockRes = await client.query(
            `SELECT block_id FROM eroll_mapping 
                     WHERE block = $1 AND block_id IS NOT NULL LIMIT 1`,
            [blockName]
          );

          if (blockRes.rows.length > 0) {
            idCache.block.set(blockName, blockRes.rows[0].block_id);
          } else {
            idCache.block.set(blockName, generateTboId('BLK'));
          }
        }
      }

      // 3️⃣ PROCESS EACH ROW FOR ID GENERATION AND INSERT
      for (const row of dataRows) {
        // Skip empty rows
        if (!row || row.length === 0) continue;

        // Create row object with headers
        const rowData = {};
        headers.forEach((header, index) => {
          if (header && row[index] !== undefined) {
            rowData[header] = row[index];
          }
        });

        // Skip if no data
        if (Object.keys(rowData).length === 0) continue;

        /* ===============================
           GENERATE BLOCK ID
        =============================== */
        if (rowData.block) {
          rowData.block_id = idCache.block.get(rowData.block);
        }

        /* ===============================
           GENERATE GP WARD ID (depends on block)
        =============================== */
        if (rowData.gp_ward && rowData.block_id) {
          const cacheKey = `${rowData.block_id}_${rowData.gp_ward}`;

          if (!idCache.gpWard.has(cacheKey)) {
            // Check if gp_ward_id already exists for this combination
            const gpWardRes = await client.query(
              `SELECT gp_ward_id FROM eroll_mapping 
                         WHERE block_id = $1 AND gp_ward = $2 AND gp_ward_id IS NOT NULL LIMIT 1`,
              [rowData.block_id, rowData.gp_ward]
            );

            if (gpWardRes.rows.length > 0) {
              idCache.gpWard.set(cacheKey, gpWardRes.rows[0].gp_ward_id);
            } else {
              const blockRef = rowData.block_id.split('-')[2];
              const gpWardId = generateTboId('GPW', [blockRef]);
              idCache.gpWard.set(cacheKey, gpWardId);
            }
          }

          rowData.gp_ward_id = idCache.gpWard.get(cacheKey);
        }

        /* ===============================
           GENERATE VILLAGE ID (depends on block AND gp_ward)
        =============================== */
        if (rowData.village && rowData.gp_ward_id) {
          const cacheKey = `${rowData.gp_ward_id}_${rowData.village}`;

          if (!idCache.village.has(cacheKey)) {
            // Check if village_id already exists for this combination
            const villageRes = await client.query(
              `SELECT village_id FROM eroll_mapping 
                         WHERE gp_ward_id = $1 AND village = $2 AND village_id IS NOT NULL LIMIT 1`,
              [rowData.gp_ward_id, rowData.village]
            );

            if (villageRes.rows.length > 0) {
              idCache.village.set(cacheKey, villageRes.rows[0].village_id);
            } else {
              const blockRef = rowData.block_id.split('-')[2];
              const gpWardRef = rowData.gp_ward_id.split('-')[3];
              const villageId = generateTboId('VIL', [blockRef, gpWardRef]);
              idCache.village.set(cacheKey, villageId);
            }
          }

          rowData.village_id = idCache.village.get(cacheKey);
        }

        /* ===============================
           GENERATE INDEPENDENT FIELD IDs
        =============================== */
        for (const field of independentFields) {
          const idField = `${field}_id`;
          const cache = idCache[field];

          if (rowData[field] && !rowData[idField]) {
            if (cache.has(rowData[field])) {
              rowData[idField] = cache.get(rowData[field]);
            } else {
              // Check if ID already exists in DB
              const idRes = await client.query(
                `SELECT ${idField} FROM eroll_mapping 
                             WHERE ${field} = $1 AND ${idField} IS NOT NULL LIMIT 1`,
                [rowData[field]]
              );

              if (idRes.rows.length > 0) {
                rowData[idField] = idRes.rows[0][idField];
                cache.set(rowData[field], rowData[idField]);
              } else {
                // Map field to type for ID generation
                const typeMap = {
                  'psb': 'PSB',
                  'coordinate': 'CRD',
                  'kendra': 'KEN',
                  'mandal': 'MAN',
                  'pjila': 'PJI',
                  'pincode': 'PIN',
                  'postoff': 'POS',
                  'policst': 'POL'
                };
                rowData[idField] = generateTboId(typeMap[field]);
                cache.set(rowData[field], rowData[idField]);
              }
            }
          }
        }

        /* ===============================
           INSERT INTO eroll_mapping
        =============================== */
        const insertValues = [];
        const insertPlaceholders = [];

        mappingColumns.forEach((col, idx) => {
          if (col === "data_id") {
            insertValues.push(dataId);
          } else {
            insertValues.push(rowData[col] !== undefined ? rowData[col] : null);
          }
          insertPlaceholders.push(`$${idx + 1}`);
        });

        const insertQuery = `
                INSERT INTO eroll_mapping (
                    ${mappingColumns.join(', ')}
                ) VALUES (${insertPlaceholders.join(', ')})
            `;

        await client.query(insertQuery, insertValues);
        insertedCount++;

        /* ===============================
           SYNC TO eroll_db
        =============================== */
        const mappingJson = {};
        specialFields.forEach(field => {
          if (rowData[field]) {
            mappingJson[field] = rowData[field];
          }
        });

        const updateParts = [];
        const updateValues = [];

        // section sync
        if (rowData["section"] !== undefined && rowData["section"] !== null) {
          updateParts.push(`section = $${updateValues.length + 1}`);
          updateValues.push(rowData["section"]);
        }

        // bhag_no sync
        if (rowData["bhag_no"] !== undefined && rowData["bhag_no"] !== null) {
          updateParts.push(`bhag_no = $${updateValues.length + 1}`);
          updateValues.push(rowData["bhag_no"]);
        }

        // sec_no sync
        if (rowData["sec_no"] !== undefined && rowData["sec_no"] !== null) {
          updateParts.push(`sec_no = $${updateValues.length + 1}`);
          updateValues.push(rowData["sec_no"]);
        }

        // mapping JSONB sync
        if (Object.keys(mappingJson).length > 0) {
          updateParts.push(`mapping = COALESCE(mapping, '{}'::jsonb) || $${updateValues.length + 1}::jsonb`);
          updateValues.push(JSON.stringify(mappingJson));
        }

        if (updateParts.length > 0) {
          updateParts.push(`update_by = CURRENT_TIMESTAMP`);

          // Add WHERE clause parameters
          updateValues.push(dataId);                    // for data_id
          updateValues.push(rowData["ac_id"] || 0);     // for ac_no
          updateValues.push(rowData["bhag_no"] || 0);   // for bhag_no
          updateValues.push(rowData["sec_no"] || 0);    // for sec_no

          const updateQuery = `
                    UPDATE eroll_db
                    SET ${updateParts.join(', ')}
                    WHERE data_id = $${updateValues.length - 3}
                    AND ac_no = $${updateValues.length - 2}
                    AND bhag_no = $${updateValues.length - 1}
                    AND sec_no = $${updateValues.length}
                `;

          const result = await client.query(updateQuery, updateValues);
          if (result.rowCount > 0) {
            updatedErollCount++;
          }
        }
      }

      await client.query('COMMIT');

      return {
        inserted: insertedCount,
        updatedEroll: updatedErollCount,
        totalRows: dataRows.length,
        idsGenerated: {
          blocks: idCache.block.size,
          gpWards: idCache.gpWard.size,
          villages: idCache.village.size,
          independent: {
            psb: idCache.psb.size,
            coordinate: idCache.coordinate.size,
            kendra: idCache.kendra.size,
            mandal: idCache.mandal.size,
            pjila: idCache.pjila.size,
            pincode: idCache.pincode.size,
            postoff: idCache.postoff.size,
            policst: idCache.policst.size
          }
        }
      };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error("Processing Error:", error);
      throw error;
    } finally {
      client.release();
    }
  },

  generateExcel: async (data_id) => {
    const result = await pool.query(
      `
    SELECT
      id,
      data_id,
      ac_id,
      ac_name,
      bhag_no,
      bhag,
      sec_no,
      section,
      ru,
      village,
      village_id,
      gp_ward,
      gp_ward_id,
      block,
      block_id,
      psb,
      psb_id,
      coordinate,
      coordinate_id,
      kendra,
      kendra_id,
      mandal,
      mandal_id,
      pjila,
      pjila_id,
      pincode,
      pincode_id,
      postoff,
      postoff_id,
      policst,
      policst_id,
      is_active,
      updated_at
    FROM eroll_mapping
    WHERE data_id = $1
    ORDER BY ac_id, bhag_no, sec_no
    `,
      [data_id]
    );

    if (result.rows.length === 0) {
      throw new Error("No records found for this data_id");
    }

    const worksheet = XLSX.utils.json_to_sheet(result.rows);

    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Mapping");

    const filePath = path.join(__dirname, `../mapping_${data_id}.xlsx`);

    XLSX.writeFile(workbook, filePath);

    return filePath;
  },

  updateMappingBatch: async (updates) => {
    const client = await pool.connect();

    const { v4: uuidv4 } = require('uuid');

    // Independent fields (don't depend on anything)
    const independentFields = [
      "psb",
      "coordinate",
      "kendra",
      "mandal",
      "pjila",
      "pincode",
      "postoff",
      "policst"
    ];

    // Generate TBO ID with readable format
    const generateTboId = (type, parts = []) => {
      const uuid = uuidv4().replace(/-/g, '');

      switch (type) {
        case 'BLK': // Block
          return `TBO-BLK-${uuid.substring(0, 6)}-${uuid.substring(6, 10)}`;

        case 'GPW': // GP Ward (depends on block)
          return `TBO-GPW-${parts[0]}-${uuid.substring(0, 6)}`;

        case 'VIL': // Village (depends on block & gp ward)
          return `TBO-VIL-${parts[0]}-${parts[1]}-${uuid.substring(0, 4)}`;

        case 'PSB': // PSB (independent)
          return `TBO-PSB-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'CRD': // Coordinate (independent)
          return `TBO-CRD-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'KEN': // Kendra (independent)
          return `TBO-KEN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'MAN': // Mandal (independent)
          return `TBO-MAN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'PJI': // Pjila (independent)
          return `TBO-PJI-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'PIN': // Pincode (independent)
          return `TBO-PIN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'POS': // Postoff (independent)
          return `TBO-POS-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'POL': // Policst (independent)
          return `TBO-POL-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        default:
          return `TBO-${uuid.substring(0, 12)}`;
      }
    };

    // Caches to maintain consistency across updates
    const idCache = {
      block: new Map(),        // blockName -> blockId
      gpWard: new Map(),       // blockId_gpWardName -> gpWardId
      village: new Map(),      // gpWardId_villageName -> villageId
      psb: new Map(),          // psbName -> psbId
      coordinate: new Map(),   // coordinateName -> coordinateId
      kendra: new Map(),       // kendraName -> kendraId
      mandal: new Map(),       // mandalName -> mandalId
      pjila: new Map(),        // pjilaName -> pjilaId
      pincode: new Map(),      // pincodeValue -> pincodeId
      postoff: new Map(),      // postoffName -> postoffId
      policst: new Map()       // policstName -> policstId
    };

    try {
      await client.query("BEGIN");

      const results = [];

      for (const row of updates) {
        if (!row.id) {
          throw new Error("id is required for update");
        }

        /* ===============================
           1️⃣ FETCH OLD KEY VALUES
        =============================== */
        const existingRes = await client.query(
          `SELECT data_id, ac_id, bhag_no, sec_no, 
                        block, gp_ward, village,
                        block_id, gp_ward_id, village_id
                 FROM eroll_mapping
                 WHERE id = $1`,
          [row.id]
        );

        if (existingRes.rowCount === 0) {
          throw new Error(`No eroll_mapping row found for id=${row.id}`);
        }

        const existing = existingRes.rows[0];
        const {
          data_id: old_data_id,
          ac_id: old_ac_id,
          bhag_no: old_bhag_no,
          sec_no: old_sec_no
        } = existing;

        /* ===============================
           2️⃣ GENERATE BLOCK ID (independent)
        =============================== */
        if (row.block != null && !row.block_id) {
          if (idCache.block.has(row.block)) {
            row.block_id = idCache.block.get(row.block);
          } else {
            // Check if block_id already exists in DB for this block name
            const blockRes = await client.query(
              `SELECT block_id FROM eroll_mapping 
                         WHERE block = $1 AND block_id IS NOT NULL LIMIT 1`,
              [row.block]
            );

            if (blockRes.rows.length > 0) {
              row.block_id = blockRes.rows[0].block_id;
              idCache.block.set(row.block, row.block_id);
            } else {
              row.block_id = generateTboId('BLK');
              idCache.block.set(row.block, row.block_id);
            }
          }
        }

        /* ===============================
           3️⃣ GENERATE GP WARD ID (depends on block)
        =============================== */
        if (row.gp_ward != null && !row.gp_ward_id) {
          const blockId = row.block_id || existing.block_id;
          const blockName = row.block || existing.block;

          if (blockId && blockName) {
            const cacheKey = `${blockId}_${row.gp_ward}`;

            if (idCache.gpWard.has(cacheKey)) {
              row.gp_ward_id = idCache.gpWard.get(cacheKey);
            } else {
              // Check if gp_ward_id already exists for this combination
              const gpWardRes = await client.query(
                `SELECT gp_ward_id FROM eroll_mapping 
                             WHERE block_id = $1 AND gp_ward = $2 AND gp_ward_id IS NOT NULL LIMIT 1`,
                [blockId, row.gp_ward]
              );

              if (gpWardRes.rows.length > 0) {
                row.gp_ward_id = gpWardRes.rows[0].gp_ward_id;
              } else {
                const blockRef = blockId.split('-')[2];
                row.gp_ward_id = generateTboId('GPW', [blockRef]);
              }

              idCache.gpWard.set(cacheKey, row.gp_ward_id);
            }
          }
        }

        /* ===============================
           4️⃣ GENERATE VILLAGE ID (depends on block AND gp_ward)
        =============================== */
        if (row.village != null && !row.village_id) {
          const blockId = row.block_id || existing.block_id;
          const gpWardId = row.gp_ward_id || existing.gp_ward_id;
          const blockName = row.block || existing.block;
          const gpWardName = row.gp_ward || existing.gp_ward;

          if (blockId && gpWardId && blockName && gpWardName) {
            const cacheKey = `${gpWardId}_${row.village}`;

            if (idCache.village.has(cacheKey)) {
              row.village_id = idCache.village.get(cacheKey);
            } else {
              // Check if village_id already exists for this combination
              const villageRes = await client.query(
                `SELECT village_id FROM eroll_mapping 
                             WHERE gp_ward_id = $1 AND village = $2 AND village_id IS NOT NULL LIMIT 1`,
                [gpWardId, row.village]
              );

              if (villageRes.rows.length > 0) {
                row.village_id = villageRes.rows[0].village_id;
              } else {
                const blockRef = blockId.split('-')[2];
                const gpWardRef = gpWardId.split('-')[3];
                row.village_id = generateTboId('VIL', [blockRef, gpWardRef]);
              }

              idCache.village.set(cacheKey, row.village_id);
            }
          }
        }

        /* ===============================
           5️⃣ GENERATE INDEPENDENT FIELD IDs
        =============================== */
        for (const field of independentFields) {
          const idField = `${field}_id`;
          const cache = idCache[field];

          if (row[field] != null && !row[idField]) {
            if (cache.has(row[field])) {
              row[idField] = cache.get(row[field]);
            } else {
              // Check if ID already exists in DB
              const idRes = await client.query(
                `SELECT ${idField} FROM eroll_mapping 
                             WHERE ${field} = $1 AND ${idField} IS NOT NULL LIMIT 1`,
                [row[field]]
              );

              if (idRes.rows.length > 0) {
                row[idField] = idRes.rows[0][idField];
              } else {
                // Map field to type for ID generation
                const typeMap = {
                  'psb': 'PSB',
                  'coordinate': 'CRD',
                  'kendra': 'KEN',
                  'mandal': 'MAN',
                  'pjila': 'PJI',
                  'pincode': 'PIN',
                  'postoff': 'POS',
                  'policst': 'POL'
                };
                row[idField] = generateTboId(typeMap[field]);
              }

              cache.set(row[field], row[idField]);
            }
          }
        }

        /* ===============================
           6️⃣ UPDATE eroll_mapping
        =============================== */
        const mapSet = [];
        const mapValues = [];
        let idx = 1;

        const editableFields = ["bhag_no", "bhag", "sec_no", "section", "ru"];

        // Add editable fields
        for (const field of editableFields) {
          if (row[field] != null) {
            mapSet.push(`${field} = $${idx++}`);
            mapValues.push(row[field]);
          }
        }

        // Add block, gp_ward, village (hierarchical fields)
        const hierarchicalFields = ["block", "gp_ward", "village"];
        for (const field of hierarchicalFields) {
          const idField = `${field}_id`;

          if (row[field] != null) {
            mapSet.push(`${field} = $${idx++}`);
            mapValues.push(row[field]);
          }

          if (row[idField] != null) {
            mapSet.push(`${idField} = $${idx++}`);
            mapValues.push(row[idField]);
          }
        }

        // Add independent fields
        for (const field of independentFields) {
          const idField = `${field}_id`;

          if (row[field] != null) {
            mapSet.push(`${field} = $${idx++}`);
            mapValues.push(row[field]);
          }

          if (row[idField] != null) {
            mapSet.push(`${idField} = $${idx++}`);
            mapValues.push(row[idField]);
          }
        }

        let mappingUpdated = 0;

        if (mapSet.length > 0) {
          mapValues.push(row.id);

          const mappingRes = await client.query(
            `UPDATE eroll_mapping
                     SET ${mapSet.join(", ")},
                         updated_at = CURRENT_TIMESTAMP
                     WHERE id = $${idx}`,
            mapValues
          );

          mappingUpdated = mappingRes.rowCount;
        }

        /* ===============================
           7️⃣ UPDATE eroll_db
        =============================== */
        const dbSet = [];
        const dbValues = [];
        let dbIdx = 1;

        if (row.section != null) {
          dbSet.push(`section = $${dbIdx++}`);
          dbValues.push(row.section);
        }

        if (row.bhag_no != null) {
          dbSet.push(`bhag_no = $${dbIdx++}`);
          dbValues.push(row.bhag_no);
        }

        if (row.sec_no != null) {
          dbSet.push(`sec_no = $${dbIdx++}`);
          dbValues.push(row.sec_no);
        }

        // Build mapping JSONB with all IDs
        let mappingExpression = "COALESCE(mapping, '{}'::jsonb)";

        // Add all ID fields to mapping JSONB
        const allIdFields = [
          "block_id", "gp_ward_id", "village_id",
          ...independentFields.map(f => `${f}_id`)
        ];

        for (const idField of allIdFields) {
          if (row[idField] != null) {
            mappingExpression = `
                        jsonb_set(
                            ${mappingExpression},
                            '{${idField}}',
                            to_jsonb($${dbIdx++}::text),
                            true
                        )
                    `;
            dbValues.push(row[idField]);
          }
        }

        if (mappingExpression !== "COALESCE(mapping, '{}'::jsonb)") {
          dbSet.push(`mapping = ${mappingExpression}`);
        }

        let dbUpdated = 0;

        if (dbSet.length > 0) {
          dbSet.push(`update_by = CURRENT_TIMESTAMP`);

          dbValues.push(
            old_data_id,
            old_ac_id,
            old_bhag_no,
            old_sec_no
          );

          const dbRes = await client.query(
            `UPDATE eroll_db
                     SET ${dbSet.join(", ")}
                     WHERE data_id = $${dbIdx}
                       AND ac_no = $${dbIdx + 1}
                       AND bhag_no = $${dbIdx + 2}
                       AND sec_no = $${dbIdx + 3}`,
            dbValues
          );

          dbUpdated = dbRes.rowCount;

          if (dbUpdated === 0) {
            console.warn(
              `No eroll_db row found for data_id=${old_data_id}, ac_no=${old_ac_id}, bhag_no=${old_bhag_no}, sec_no=${old_sec_no}`
            );
          }
        }

        results.push({
          id: row.id,
          mapping_updated: mappingUpdated,
          db_updated: dbUpdated,
          old_values: {
            data_id: old_data_id,
            ac_id: old_ac_id,
            bhag_no: old_bhag_no,
            sec_no: old_sec_no
          },
          new_values: {
            bhag_no: row.bhag_no || old_bhag_no,
            sec_no: row.sec_no || old_sec_no,
            section: row.section
          },
          generated_ids: {
            block_id: row.block_id,
            gp_ward_id: row.gp_ward_id,
            village_id: row.village_id,
            ...independentFields.reduce((acc, field) => {
              const idField = `${field}_id`;
              if (row[idField]) acc[idField] = row[idField];
              return acc;
            }, {})
          }
        });
      }

      await client.query("COMMIT");

      return {
        success: true,
        total_processed: updates.length,
        details: results
      };

    } catch (err) {
      await client.query("ROLLBACK");
      console.error("Update Mapping Batch Error:", err);
      throw err;
    } finally {
      client.release();
    }
  },

  syncMappingToErollDb: async (dataId) => {
    const client = await pool.connect();

    const { v4: uuidv4 } = require('uuid');

    // Generate TBO ID with readable format
    const generateTboId = (type, parts = []) => {
      const uuid = uuidv4().replace(/-/g, '');

      switch (type) {
        case 'BLK': // Block
          return `TBO-BLK-${uuid.substring(0, 6)}-${uuid.substring(6, 10)}`;

        case 'GPW': // GP Ward (depends on block)
          return `TBO-GPW-${parts[0]}-${uuid.substring(0, 6)}`;

        case 'VIL': // Village (depends on block & gp ward)
          return `TBO-VIL-${parts[0]}-${parts[1]}-${uuid.substring(0, 4)}`;

        case 'PSB': // PSB (independent)
          return `TBO-PSB-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'CRD': // Coordinate (independent)
          return `TBO-CRD-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'KEN': // Kendra (independent)
          return `TBO-KEN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'MAN': // Mandal (independent)
          return `TBO-MAN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'PJI': // Pjila (independent)
          return `TBO-PJI-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'PIN': // Pincode (independent)
          return `TBO-PIN-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'POS': // Postoff (independent)
          return `TBO-POS-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        case 'POL': // Policst (independent)
          return `TBO-POL-${uuid.substring(0, 8)}-${uuid.substring(8, 12)}`;

        default:
          return `TBO-${uuid.substring(0, 12)}`;
      }
    };

    const caches = {
      block: new Map(),
      gpWard: new Map(),
      village: new Map(),
      psb: new Map(),
      coordinate: new Map(),
      kendra: new Map(),
      mandal: new Map(),
      pjila: new Map(),
      pincode: new Map(),
      postoff: new Map(),
      policst: new Map()
    };

    try {
      await client.query('BEGIN');
      const independentFields = [
        { field: 'block', type: 'BLK', cache: 'block' },
        { field: 'psb', type: 'PSB', cache: 'psb' },
        { field: 'coordinate', type: 'CRD', cache: 'coordinate' },
        { field: 'kendra', type: 'KEN', cache: 'kendra' },
        { field: 'mandal', type: 'MAN', cache: 'mandal' },
        { field: 'pjila', type: 'PJI', cache: 'pjila' },
        { field: 'pincode', type: 'PIN', cache: 'pincode' },
        { field: 'postoff', type: 'POS', cache: 'postoff' },
        { field: 'policst', type: 'POL', cache: 'policst' }
      ];

      // Generate IDs for all independent fields
      for (const { field, type, cache } of independentFields) {
        const result = await client.query(`
                SELECT DISTINCT ${field} 
                FROM eroll_mapping 
                WHERE data_id = $1 AND ${field} IS NOT NULL
            `, [dataId]);

        for (const row of result.rows) {
          const value = row[field];
          if (!caches[cache].has(value)) {
            const newId = generateTboId(type);
            caches[cache].set(value, newId);
          }
        }
      }

      const mappingQuery = `
            SELECT 
                id,
                data_id,
                ac_id,
                bhag_no,
                bhag,
                sec_no,
                section,
                ru,
                block,
                gp_ward,
                village,
                psb,
                coordinate,
                kendra,
                mandal,
                pjila,
                pincode,
                postoff,
                policst,
                -- Include existing IDs if any
                village_id,
                gp_ward_id,
                block_id,
                psb_id,
                coordinate_id,
                kendra_id,
                mandal_id,
                pjila_id,
                pincode_id,
                postoff_id,
                policst_id
            FROM eroll_mapping 
            WHERE data_id = $1
        `;

      const mappingRes = await client.query(mappingQuery, [dataId]);

      if (mappingRes.rows.length === 0) {
        throw new Error(`No mapping records found for data_id ${dataId}`);
      }

      const stats = {
        total: mappingRes.rows.length,
        updated: 0,
        failed: 0,
        ids_generated: {
          blocks: caches.block.size,
          gp_wards: 0,
          villages: 0,
          psb: caches.psb.size,
          coordinate: caches.coordinate.size,
          kendra: caches.kendra.size,
          mandal: caches.mandal.size,
          pjila: caches.pjila.size,
          pincode: caches.pincode.size,
          postoff: caches.postoff.size,
          policst: caches.policst.size
        },
        details: []
      };

      // ===== 3. PROCESS EACH RECORD =====
      for (const mapping of mappingRes.rows) {
        try {
          // ===== BLOCK ID (independent) =====
          let blockId = mapping.block_id;
          if (mapping.block && !blockId) {
            blockId = caches.block.get(mapping.block);
            await client.query(
              `UPDATE eroll_mapping SET block_id = $1 WHERE id = $2`,
              [blockId, mapping.id]
            );
          }

          // ===== GP WARD ID (depends on block) =====
          let gpWardId = mapping.gp_ward_id;
          if (mapping.gp_ward && blockId && !gpWardId) {
            const cacheKey = `${blockId}_${mapping.gp_ward}`;

            if (!caches.gpWard.has(cacheKey)) {
              const blockRef = blockId.split('-')[2];
              const newGpWardId = generateTboId('GPW', [blockRef]);
              caches.gpWard.set(cacheKey, newGpWardId);
              stats.ids_generated.gp_wards++;

              await client.query(
                `UPDATE eroll_mapping SET gp_ward_id = $1 WHERE id = $2`,
                [newGpWardId, mapping.id]
              );

              gpWardId = newGpWardId;
            } else {
              gpWardId = caches.gpWard.get(cacheKey);
            }
          }

          // ===== VILLAGE ID (depends on block AND gp_ward) =====
          let villageId = mapping.village_id;
          if (mapping.village && blockId && gpWardId && !villageId) {
            const cacheKey = `${gpWardId}_${mapping.village}`;

            if (!caches.village.has(cacheKey)) {
              const blockRef = blockId.split('-')[2];
              const gpWardRef = gpWardId.split('-')[3];
              const newVillageId = generateTboId('VIL', [blockRef, gpWardRef]);
              caches.village.set(cacheKey, newVillageId);
              stats.ids_generated.villages++;

              await client.query(
                `UPDATE eroll_mapping SET village_id = $1 WHERE id = $2`,
                [newVillageId, mapping.id]
              );

              villageId = newVillageId;
            } else {
              villageId = caches.village.get(cacheKey);
            }
          }

          // ===== INDEPENDENT FIELD IDs =====
          const independentMappings = [
            { field: 'psb', idField: 'psb_id', cache: caches.psb, type: 'PSB' },
            { field: 'coordinate', idField: 'coordinate_id', cache: caches.coordinate, type: 'CRD' },
            { field: 'kendra', idField: 'kendra_id', cache: caches.kendra, type: 'KEN' },
            { field: 'mandal', idField: 'mandal_id', cache: caches.mandal, type: 'MAN' },
            { field: 'pjila', idField: 'pjila_id', cache: caches.pjila, type: 'PJI' },
            { field: 'pincode', idField: 'pincode_id', cache: caches.pincode, type: 'PIN' },
            { field: 'postoff', idField: 'postoff_id', cache: caches.postoff, type: 'POS' },
            { field: 'policst', idField: 'policst_id', cache: caches.policst, type: 'POL' }
          ];

          for (const { field, idField, cache } of independentMappings) {
            if (mapping[field] && !mapping[idField]) {
              const id = cache.get(mapping[field]);
              if (id) {
                await client.query(
                  `UPDATE eroll_mapping SET ${idField} = $1 WHERE id = $2`,
                  [id, mapping.id]
                );
                mapping[idField] = id;
              }
            }
          }

          // ===== BUILD MAPPING JSON FOR EROLL_DB =====
          const mappingJson = {};

          // Add all IDs to mapping JSON
          const allIdFields = [
            'block_id', 'gp_ward_id', 'village_id',
            'psb_id', 'coordinate_id', 'kendra_id',
            'mandal_id', 'pjila_id', 'pincode_id',
            'postoff_id', 'policst_id'
          ];

          allIdFields.forEach(field => {
            if (mapping[field] || (field === 'block_id' && blockId) ||
              (field === 'gp_ward_id' && gpWardId) ||
              (field === 'village_id' && villageId)) {

              let value = mapping[field];
              if (field === 'block_id' && !value) value = blockId;
              if (field === 'gp_ward_id' && !value) value = gpWardId;
              if (field === 'village_id' && !value) value = villageId;

              if (value) {
                mappingJson[field] = value;
              }
            }
          });

          // ===== UPDATE EROLL_DB =====
          const updateParts = [];
          const updateValues = [];
          let paramIndex = 1;

          // Update direct fields
          if (mapping.section) {
            updateParts.push(`section = $${paramIndex++}`);
            updateValues.push(mapping.section);
          }

          if (mapping.bhag_no) {
            updateParts.push(`bhag_no = $${paramIndex++}`);
            updateValues.push(mapping.bhag_no);
          }

          if (mapping.sec_no) {
            updateParts.push(`sec_no = $${paramIndex++}`);
            updateValues.push(mapping.sec_no);
          }

          // Update mapping JSONB
          if (Object.keys(mappingJson).length > 0) {
            updateParts.push(`mapping = COALESCE(mapping, '{}'::jsonb) || $${paramIndex++}::jsonb`);
            updateValues.push(JSON.stringify(mappingJson));
          }

          // Always update timestamp
          updateParts.push(`update_by = CURRENT_TIMESTAMP`);

          // Add WHERE clause parameters
          updateValues.push(dataId);                    // data_id
          updateValues.push(mapping.ac_id);             // ac_no
          updateValues.push(mapping.bhag_no);           // bhag_no
          updateValues.push(mapping.sec_no);            // sec_no

          if (updateParts.length > 1) {
            const updateQuery = `
                        UPDATE eroll_db 
                        SET ${updateParts.join(', ')}
                        WHERE data_id = $${paramIndex}
                          AND ac_no = $${paramIndex + 1}
                          AND bhag_no = $${paramIndex + 2}
                          AND sec_no = $${paramIndex + 3}
                    `;

            const updateRes = await client.query(updateQuery, updateValues);

            stats.details.push({
              id: mapping.id,
              ac_id: mapping.ac_id,
              bhag_no: mapping.bhag_no,
              sec_no: mapping.sec_no,
              updated: updateRes.rowCount > 0,
              rowsAffected: updateRes.rowCount,
              ids: mappingJson
            });

            if (updateRes.rowCount > 0) {
              stats.updated++;
            } else {
              stats.failed++;
            }
          }

        } catch (rowError) {
          console.error(`Error syncing mapping id ${mapping.id}:`, rowError);
          stats.failed++;
          stats.details.push({
            id: mapping.id,
            error: rowError.message
          });
        }
      }

      await client.query('COMMIT');

      return {
        success: true,
        data_id: dataId,
        ...stats
      };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error("Sync Mapping Error:", error);
      throw error;
    } finally {
      client.release();
    }
  },

  updateMappingFromDbBatch: async (updates) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const specialFields = [
        "village", "gp_ward", "block", "psb",
        "coordinate", "kendra", "mandal", "pjila",
        "pincode", "postoff", "policst"
      ];

      const updatedRows = [];

      for (const row of updates) {
        const { data_id, section } = row;

        const dbRowsRes = await client.query(
          `SELECT mapping, ${specialFields.join(", ")} FROM eroll_db WHERE data_id = $1 AND section = $2`,
          [data_id, section]
        );

        if (dbRowsRes.rows.length === 0) continue;

        const dbRow = dbRowsRes.rows[0];
        const mappingJson = dbRow.mapping || {};
        const idUpdates = {};

        for (const field of specialFields) {
          const idField = `${field}_id`;

          if (!mappingJson[idField]) {
            const newId = uuidv4();
            mappingJson[idField] = newId;
            idUpdates[idField] = newId;
          }
        }

        const setClauses = [];
        const values = [];
        let idx = 1;

        for (const field of specialFields) {
          if (dbRow[field] !== undefined) {
            setClauses.push(`${field} = $${idx}`);
            values.push(dbRow[field]);
            idx++;
          }
          const idField = `${field}_id`;
          if (idUpdates[idField]) {
            setClauses.push(`${idField} = $${idx}`);
            values.push(idUpdates[idField]);
            idx++;
          }
        }

        values.push(data_id, section);
        const updateMappingQuery = `
                UPDATE eroll_mapping
                SET ${setClauses.join(", ")}
                WHERE data_id = $${idx} AND section = $${idx + 1}
                RETURNING *;
            `;

        const mappingResult = await client.query(updateMappingQuery, values);
        updatedRows.push(mappingResult.rows[0]);

        if (Object.keys(idUpdates).length > 0) {
          await client.query(
            `UPDATE eroll_db SET mapping = $1 WHERE data_id = $2 AND section = $3`,
            [mappingJson, data_id, section]
          );
        }
      }

      await client.query("COMMIT");
      return updatedRows;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  },

  //sync dataid
  syncByDataIdBatch: async (updates) => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const results = [];

      for (const row of updates) {
        const { data_id, status, ...fields } = row;

        if (!data_id) continue;

        const setClauses = [];
        const values = [];
        let idx = 1;

        for (const [key, value] of Object.entries(fields)) {
          if (value !== undefined) {
            setClauses.push(`${key} = $${idx}`);
            values.push(value);
            idx++;
          }
        }

        if (setClauses.length === 0) continue;

        values.push(data_id);

        const updateMasterQuery = `
        UPDATE dataid_importmaster
        SET ${setClauses.join(", ")},
            updated_at = CURRENT_TIMESTAMP
        WHERE data_id = $${idx}
        RETURNING *;
      `;

        const masterResult = await client.query(updateMasterQuery, values);

        const mappingQuery = `
        UPDATE eroll_mapping
        SET ${setClauses.join(", ")},
            updated_at = CURRENT_DATE
        WHERE data_id = $${idx};
      `;

        await client.query(mappingQuery, values);

        const dbQuery = `
        UPDATE eroll_db
        SET ${setClauses.join(", ")},
            update_by = CURRENT_TIMESTAMP
        WHERE data_id = $${idx};
      `;

        await client.query(dbQuery, values);

        results.push(masterResult.rows[0]);
      }

      await client.query("COMMIT");

      try {
        await refreshGlobalCache();
      } catch (cacheError) {
        console.error("Failed to refresh cache:", cacheError);
      }
      return results;

    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  },

  getImportMasterForExcel: async (filters = {}) => {
    try {
      const conditions = [];
      const values = [];
      let index = 1;

      if (filters.data_id !== undefined && filters.data_id !== "") {
        conditions.push(`data_id = $${index}`);
        values.push(Number(filters.data_id));
        index++;
      }

      if (filters.ac_no !== undefined && filters.ac_no !== "") {
        conditions.push(`ac_no = $${index}`);
        values.push(Number(filters.ac_no));
        index++;
      }

      if (filters.pc_no !== undefined && filters.pc_no !== "") {
        conditions.push(`pc_no = $${index}`);
        values.push(Number(filters.pc_no));
        index++;
      }

      if (filters.district_id !== undefined && filters.district_id !== "") {
        conditions.push(`district_id = $${index}`);
        values.push(Number(filters.district_id));
        index++;
      }

      if (
        filters.party_district_id !== undefined &&
        filters.party_district_id !== ""
      ) {
        conditions.push(`party_district_id = $${index}`);
        values.push(Number(filters.party_district_id));
        index++;
      }

      if (filters.is_active !== undefined && filters.is_active !== "") {
        conditions.push(`is_active = $${index}`);
        values.push(Number(filters.is_active));
        index++;
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const query = `
        SELECT
          id,
          data_id,
          data_id_name_hi,
          data_id_name_en,
          ac_no,
          ac_name_en,
          ac_name_hi,
          pc_no,
          pc_name_en,
          pc_name_hi,
          district_id,
          district_en,
          district_hi,
          party_district_id,
          party_district_hi,
          party_district_en,
          div_id,
          div_name_en,
          div_name_hi,
          data_range,
          is_active,
          updated_at
        FROM dataid_importmaster
        ${whereClause}
        ORDER BY id DESC
      `;

      const result = await pool.query(query, values);
      return result.rows;
    } catch (error) {
      console.log("getImportMasterForExcel error => ", error);
      throw error;
    }
  },

  generateSurname: async (dataIds) => {
    try {
      const client = await pool.connect();

      const query = `
      UPDATE eroll_db
      SET surname = jsonb_build_object(
        'v',
        CASE
          WHEN array_length(string_to_array(trim(vname), ' '),1) > 1
            THEN split_part(trim(vname),' ', array_length(string_to_array(trim(vname),' '),1))
          ELSE NULL
        END,
        'r',
        CASE
          WHEN array_length(string_to_array(trim(rname), ' '),1) > 1
            THEN split_part(trim(rname),' ', array_length(string_to_array(trim(rname),' '),1))
          ELSE NULL
        END
      )
      WHERE data_id = ANY($1)
    `;

      await client.query(query, [dataIds]);
      client.release();

      return true;
    } catch (err) {
      console.error("Error generating surnames:", err);
      throw err;
    }
  },

  generateFamilyIds: async (dataIds) => {
    const client = await pool.connect();

    try {
      if (!Array.isArray(dataIds) || dataIds.length === 0) {
        return {
          success: false,
          message: 'dataIds must be a non-empty array'
        };
      }

      const cleanIds = [
        ...new Set(
          dataIds
            .map((id) => Number(id))
            .filter((id) => Number.isInteger(id) && id > 0)
        )
      ];

      if (cleanIds.length === 0) {
        return {
          success: false,
          message: 'No valid dataIds found'
        };
      }

      await client.query('BEGIN');

      const selectRes = await client.query(
        `
      SELECT
        id,
        data_id,
        ac_no,
        bhag_no,
        sec_no,
        hno
      FROM eroll_db
      WHERE data_id = ANY($1::int[])
      ORDER BY id ASC
      `,
        [cleanIds]
      );

      const rows = selectRes.rows;

      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return {
          success: false,
          message: 'No rows found in eroll_db'
        };
      }

      const familyState = new Map();
      let nextFamilyId = 1;

      for (const row of rows) {
        const acNo = row.ac_no !== null && row.ac_no !== undefined
          ? String(row.ac_no).trim()
          : '';

        const bhagNo = row.bhag_no !== null && row.bhag_no !== undefined
          ? String(row.bhag_no).trim()
          : '';

        const secNo = row.sec_no !== null && row.sec_no !== undefined
          ? String(row.sec_no).trim()
          : '';

        const housePart = row.hno !== null && row.hno !== undefined && String(row.hno).trim() !== ''
          ? String(row.hno).trim().replace(/\s+/g, '')
          : 'NOHOUSE';

        const key = `${acNo}||${bhagNo}||${secNo}||${housePart}`;

        if (!familyState.has(key)) {
          familyState.set(key, nextFamilyId);
          nextFamilyId++;
        }

        row.computed_family_id = familyState.get(key);
      }

      let updatedCount = 0;

      for (const row of rows) {
        const updateRes = await client.query(
          `
        UPDATE eroll_db
        SET
          familyId = $1,
          update_by = CURRENT_TIMESTAMP
        WHERE id = $2
        `,
          [row.computed_family_id, row.id]
        );

        updatedCount += updateRes.rowCount;
      }

      await client.query('COMMIT');

      return {
        success: true,
        message: 'Family IDs generated successfully',
        data_ids: cleanIds,
        total_rows: rows.length,
        total_updated: updatedCount,
        total_unique_families: familyState.size
      };
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('generateFamilyIds ERROR:', error);
      throw error;
    } finally {
      client.release();
    }
  },

  getRegister: async (data_id) => {
    const query = `
      SELECT reg_name from eroll_yojna_master where data_id = $1
    `
    const result = await pool.query(query, [data_id])
    return result.rows
  },

  generateMappingids: async (data_ids = []) => {
    const client = await pool.connect();

    const FIELD_CONFIG = [
      { source: 'block', target: 'block_id' },
      { source: 'gp_ward', target: 'gp_ward_id' },
      { source: 'village', target: 'village_id' },
      { source: 'psb', target: 'psb_id' },
      { source: 'coordinate', target: 'coordinate_id' },
      { source: 'kendra', target: 'kendra_id' },
      { source: 'mandal', target: 'mandal_id' },
      { source: 'pjila', target: 'pjila_id' },
      { source: 'pincode', target: 'pincode_id' },
      { source: 'postoff', target: 'postoff_id' },
      { source: 'policst', target: 'policst_id' }
    ];

    const normalize = (val) => {
      if (val === null || val === undefined) return '';
      return String(val).trim().replace(/\s+/g, ' ').toLowerCase();
    };

    const randomId = () => `TBO${Math.floor(100000 + Math.random() * 900000)}`;

    const generateUniqueIdForColumn = async (targetColumn, usedIds) => {
      let attempts = 0;
      let newId = null;
      let isUnique = false;

      while (!isUnique && attempts < 500) {
        attempts++;
        newId = randomId();

        if (usedIds.has(newId)) {
          continue;
        }

        const checkRes = await client.query(
          `SELECT 1 FROM eroll_mapping WHERE ${targetColumn} = $1 LIMIT 1`,
          [newId]
        );

        if (checkRes.rows.length === 0) {
          isUnique = true;
        }
      }

      if (!isUnique) {
        throw new Error(`Could not generate unique ID for column ${targetColumn}`);
      }

      usedIds.add(newId);
      return newId;
    };

    try {
      if (!Array.isArray(data_ids) || data_ids.length === 0) {
        return {
          success: false,
          message: 'data_ids must be a non-empty array'
        };
      }

      const cleanIds = [
        ...new Set(
          data_ids
            .map((id) => Number(id))
            .filter((id) => Number.isInteger(id) && id > 0)
        )
      ];

      if (cleanIds.length === 0) {
        return {
          success: false,
          message: 'No valid data_ids found'
        };
      }

      await client.query('BEGIN');

      const mappingRes = await client.query(
        `
      SELECT
        id,
        data_id,
        ac_id,
        bhag_no,
        sec_no,
        block,
        block_id,
        gp_ward,
        gp_ward_id,
        village,
        village_id,
        psb,
        psb_id,
        coordinate,
        coordinate_id,
        kendra,
        kendra_id,
        mandal,
        mandal_id,
        pjila,
        pjila_id,
        pincode,
        pincode_id,
        postoff,
        postoff_id,
        policst,
        policst_id
      FROM eroll_mapping
      WHERE data_id = ANY($1::int[])
      ORDER BY id ASC
      `,
        [cleanIds]
      );

      const rows = mappingRes.rows;

      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return {
          success: false,
          message: 'No rows found in eroll_mapping'
        };
      }

      console.log('generateMappingids cleanIds =>', cleanIds);
      console.log('generateMappingids rows found =>', rows.length);

      let totalGenerated = 0;
      let totalSaved = 0;
      const generatedPreview = {};

      for (const config of FIELD_CONFIG) {
        const valueToIdMap = new Map();
        const usedIdsInThisColumn = new Set();

        generatedPreview[config.target] = [];

        const existingIdsRes = await client.query(
          `
        SELECT ${config.target}
        FROM eroll_mapping
        WHERE ${config.target} IS NOT NULL
          AND TRIM(${config.target}::text) <> ''
        `
        );

        for (const item of existingIdsRes.rows) {
          if (item[config.target]) {
            usedIdsInThisColumn.add(String(item[config.target]).trim());
          }
        }

        for (const row of rows) {
          const sourceValue = normalize(row[config.source]);

          if (!sourceValue) continue;

          const mapKey = `${row.data_id}__${sourceValue}`;

          if (!valueToIdMap.has(mapKey)) {
            const newId = await generateUniqueIdForColumn(
              config.target,
              usedIdsInThisColumn
            );

            valueToIdMap.set(mapKey, newId);
            totalGenerated++;

            generatedPreview[config.target].push({
              data_id: row.data_id,
              value: row[config.source],
              generated_id: newId
            });
          }
        }

        for (const row of rows) {
          const sourceValue = normalize(row[config.source]);

          if (!sourceValue) continue;

          const mapKey = `${row.data_id}__${sourceValue}`;
          const mappedId = valueToIdMap.get(mapKey);

          if (!mappedId) continue;

          const updateRes = await client.query(
            `
          UPDATE eroll_mapping
          SET ${config.target} = $1,
              update_by = CURRENT_TIMESTAMP
          WHERE data_id = $2
            AND LOWER(REGEXP_REPLACE(TRIM(COALESCE(${config.source}, '')), '\\s+', ' ', 'g')) = $3
          `,
            [mappedId, row.data_id, sourceValue]
          );

          totalSaved += updateRes.rowCount;
        }

        console.log(
          `generateMappingids ${config.target} generated =>`,
          generatedPreview[config.target]
        );
      }

      const syncRes = await client.query(
        `
      UPDATE eroll_db ed
      SET
        mapping = jsonb_strip_nulls(
          COALESCE(ed.mapping, '{}'::jsonb)
          || jsonb_build_object(
            'block_id', em.block_id,
            'gp_ward_id', em.gp_ward_id,
            'village_id', em.village_id,
            'psb_id', em.psb_id,
            'coordinate_id', em.coordinate_id,
            'kendra_id', em.kendra_id,
            'mandal_id', em.mandal_id,
            'pjila_id', em.pjila_id,
            'pincode_id', em.pincode_id,
            'postoff_id', em.postoff_id,
            'policst_id', em.policst_id
          )
        ),
        update_by = CURRENT_TIMESTAMP
      FROM eroll_mapping em
      WHERE ed.data_id = em.data_id
        AND ed.ac_no = em.ac_id
        AND ed.bhag_no = em.bhag_no
        AND ed.sec_no = em.sec_no
        AND em.data_id = ANY($1::int[])
      `,
        [cleanIds]
      );

      console.log('generateMappingids totalGenerated =>', totalGenerated);
      console.log('generateMappingids totalSaved =>', totalSaved);
      console.log('generateMappingids total_eroll_db_synced =>', syncRes.rowCount);

      await client.query('COMMIT');

      return {
        success: true,
        message: 'Mapping IDs regenerated successfully, saved in eroll_mapping, and synced to eroll_db',
        data_ids: cleanIds,
        total_rows: rows.length,
        total_generated: totalGenerated,
        total_saved_in_mapping: totalSaved,
        total_eroll_db_synced: syncRes.rowCount,
        generated_preview: generatedPreview
      };
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('generateMappingids ERROR:', error);
      throw error;
    } finally {
      client.release();
    }
  },

  getYojnaList: async (dataId) => {
    try {
      const query = `
        SELECT 
          id,
          yojna_id,
          yojna_name,
          regid,
          reg_name,
          data_id,
          is_active,
          updated_at
        FROM eroll_yojna_master
        WHERE data_id = $1
        ORDER BY id ASC
      `;
      const result = await pool.query(query, [dataId]);
      return result.rows;
    } catch (err) {
      console.error("Error fetching yojna list:", err);
      throw err;
    }
  },

  getErollMappingForExcel: async (filters = {}) => {
    try {
      const conditions = [];
      const values = [];
      let index = 1;

      if (filters.data_id !== undefined && filters.data_id !== "") {
        conditions.push(`data_id = $${index}`);
        values.push(Number(filters.data_id));
        index++;
      }

      if (filters.is_active !== undefined && filters.is_active !== "") {
        conditions.push(`is_active = $${index}`);
        values.push(Number(filters.is_active));
        index++;
      }

      if (filters.ac_id !== undefined && filters.ac_id !== "") {
        conditions.push(`ac_id = $${index}`);
        values.push(Number(filters.ac_id));
        index++;
      }

      if (filters.bhag_no !== undefined && filters.bhag_no !== "") {
        conditions.push(`bhag_no = $${index}`);
        values.push(Number(filters.bhag_no));
        index++;
      }

      if (filters.sec_no !== undefined && filters.sec_no !== "") {
        conditions.push(`sec_no = $${index}`);
        values.push(Number(filters.sec_no));
        index++;
      }

      const textFields = [
        "village",
        "gp_ward",
        "block",
        "psb",
        "coordinate",
        "kendra",
        "mandal",
        "pjila",
        "postoff",
        "policst"
      ];

      for (const field of textFields) {
        if (filters[field] !== undefined && filters[field] !== "") {
          conditions.push(`${field} ILIKE $${index}`);
          values.push(`%${filters[field]}%`);
          index++;
        }
      }

      if (filters.search !== undefined && filters.search !== "") {
        conditions.push(`
          (
            CAST(data_id AS TEXT) ILIKE $${index}
            OR CAST(ac_id AS TEXT) ILIKE $${index}
            OR CAST(bhag_no AS TEXT) ILIKE $${index}
            OR CAST(sec_no AS TEXT) ILIKE $${index}
            OR COALESCE(ac_name, '') ILIKE $${index}
            OR COALESCE(bhag, '') ILIKE $${index}
            OR COALESCE(section, '') ILIKE $${index}
            OR COALESCE(village, '') ILIKE $${index}
            OR COALESCE(gp_ward, '') ILIKE $${index}
            OR COALESCE(block, '') ILIKE $${index}
            OR COALESCE(psb, '') ILIKE $${index}
            OR COALESCE(coordinate, '') ILIKE $${index}
            OR COALESCE(kendra, '') ILIKE $${index}
            OR COALESCE(mandal, '') ILIKE $${index}
            OR COALESCE(pjila, '') ILIKE $${index}
            OR COALESCE(postoff, '') ILIKE $${index}
            OR COALESCE(policst, '') ILIKE $${index}
            OR COALESCE(village_id, '') ILIKE $${index}
            OR COALESCE(gp_ward_id, '') ILIKE $${index}
            OR COALESCE(block_id, '') ILIKE $${index}
            OR COALESCE(psb_id, '') ILIKE $${index}
            OR COALESCE(coordinate_id, '') ILIKE $${index}
            OR COALESCE(kendra_id, '') ILIKE $${index}
            OR COALESCE(mandal_id, '') ILIKE $${index}
            OR COALESCE(pjila_id, '') ILIKE $${index}
            OR COALESCE(pincode_id, '') ILIKE $${index}
            OR COALESCE(postoff_id, '') ILIKE $${index}
            OR COALESCE(policst_id, '') ILIKE $${index}
          )
        `);
        values.push(`%${filters.search}%`);
        index++;
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const query = `
        SELECT
          id,
          data_id,
          ac_id,
          ac_name,
          bhag_no,
          bhag,
          sec_no,
          section,
          ru,
          village,
          gp_ward,
          block,
          psb,
          coordinate,
          kendra,
          mandal,
          pjila,
          pincode,
          postoff,
          policst,
          is_active,
          updated_at,
          village_id,
          gp_ward_id,
          block_id,
          psb_id,
          coordinate_id,
          kendra_id,
          mandal_id,
          pjila_id,
          pincode_id,
          postoff_id,
          policst_id,
          update_by
        FROM eroll_mapping
        ${whereClause}
        ORDER BY id DESC
      `;

      const result = await pool.query(query, values);
      return result.rows;
    } catch (error) {
      console.log("getErollMappingForExcel error => ", error);
      throw error;
    }
  },

  insertEmptyImportMasterRow: async () => {
    try {
      const query = `
      INSERT INTO dataid_importmaster (
        data_id,
        data_id_name_hi,
        data_id_name_en,
        ac_no,
        ac_name_en,
        ac_name_hi,
        pc_no,
        pc_name_en,
        pc_name_hi,
        district_id,
        district_en,
        district_hi,
        party_district_id,
        party_district_hi,
        party_district_en,
        div_id,
        div_name_en,
        div_name_hi,
        data_range,
        is_active,
        updated_at
      )
      VALUES (
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        CURRENT_TIMESTAMP
      )
      RETURNING *;
    `;

      const result = await pool.query(query);
      return result.rows[0];
    } catch (error) {
      console.log("insertEmptyImportMasterRow error => ", error);
      throw error;
    }
  },

  syncSurname: async (req) => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const processedBy = req.user?.id || 0;

      const dataIds = Array.isArray(req.body?.data_id)
        ? req.body.data_id.map(Number).filter(Boolean)
        : [];

      const acNo = req.body?.ac_no ? Number(req.body.ac_no) : null;

      if (!dataIds.length) {
        throw new Error("data_id array is required");
      }

      let deleteSql = `DELETE FROM eroll_surname WHERE data_id = ANY($1)`;
      const deleteValues = [dataIds];

      if (acNo) {
        deleteSql += ` AND ac_no = $2`;
        deleteValues.push(acNo);
      }

      await client.query(deleteSql, deleteValues);

      const values = [dataIds];
      let idx = 2;

      let whereSql = `
      WHERE ed.data_id = ANY($1)
        AND ed.surname IS NOT NULL
        AND (
          NULLIF(BTRIM(ed.surname->>'v'), '') IS NOT NULL
          OR NULLIF(BTRIM(ed.surname->>'r'), '') IS NOT NULL
        )
    `;

      if (acNo) {
        whereSql += ` AND ed.ac_no = $${idx++}`;
        values.push(acNo);
      }

      const syncSql = `
      WITH filtered AS (
        SELECT
          ed.data_id,
          ed.ac_no,
          COALESCE(dim.pc_no, 0) AS pc_no,
          COALESCE(dim.district_hi, '') AS district,
          COALESCE(em.block, '') AS block,

          NULLIF(BTRIM(ed.surname->>'v'), '') AS v_surname,
          NULLIF(BTRIM(ed.surname->>'r'), '') AS r_surname,

          NULLIF(BTRIM(ed.castid_surname), '') AS castid_surname,
          NULLIF(BTRIM(ed.castid), '') AS caste,
          NULLIF(BTRIM(ed.cast_cat), '') AS cast_cat,

          NULLIF(BTRIM(ecm.religion_hi), '') AS religion

        FROM eroll_db ed

        LEFT JOIN eroll_mapping em
          ON ed.data_id = em.data_id
         AND ed.ac_no = em.ac_id
         AND ed.bhag_no = em.bhag_no
         AND ed.sec_no = em.sec_no

        LEFT JOIN dataid_importmaster dim
          ON ed.data_id = dim.data_id

        LEFT JOIN eroll_castmaster ecm
          ON ecm.data_id = ed.data_id
         AND UPPER(BTRIM(COALESCE(ecm.castid, ''))) = UPPER(BTRIM(COALESCE(ed.castid, '')))

        ${whereSql}
      ),

      expanded AS (
        SELECT
          data_id,
          v_surname AS surname,
          1 AS v_count,
          0 AS r_count,
          castid_surname,
          caste,
          cast_cat,
          religion,
          district,
          block,
          ac_no,
          pc_no
        FROM filtered
        WHERE v_surname IS NOT NULL

        UNION ALL

        SELECT
          data_id,
          r_surname AS surname,
          0 AS v_count,
          1 AS r_count,
          castid_surname,
          caste,
          cast_cat,
          religion,
          district,
          block,
          ac_no,
          pc_no
        FROM filtered
        WHERE r_surname IS NOT NULL
      ),

      aggregated AS (
        SELECT
          data_id,
          surname,
          SUM(v_count)::INT AS v_count,
          SUM(r_count)::INT AS r_count,

          string_agg(DISTINCT castid_surname, ', ' ORDER BY castid_surname)
            FILTER (
              WHERE castid_surname IS NOT NULL
                AND BTRIM(castid_surname) <> ''
            ) AS castid_surname,

          string_agg(DISTINCT caste, ', ' ORDER BY caste)
            FILTER (
              WHERE caste IS NOT NULL
                AND BTRIM(caste) <> ''
            ) AS caste,

          string_agg(DISTINCT cast_cat, ', ' ORDER BY cast_cat)
            FILTER (
              WHERE cast_cat IS NOT NULL
                AND BTRIM(cast_cat) <> ''
            ) AS cast_cat,

          string_agg(DISTINCT religion, ', ' ORDER BY religion)
            FILTER (
              WHERE religion IS NOT NULL
                AND BTRIM(religion) <> ''
            ) AS religion,

          string_agg(DISTINCT district, ', ' ORDER BY district)
            FILTER (
              WHERE district IS NOT NULL
                AND BTRIM(district) <> ''
            ) AS district,

          string_agg(DISTINCT block, ', ' ORDER BY block)
            FILTER (
              WHERE block IS NOT NULL
                AND BTRIM(block) <> ''
            ) AS block,

          MIN(ac_no) AS ac_no,
          MIN(pc_no) AS pc_no

        FROM expanded
        WHERE surname IS NOT NULL
          AND BTRIM(surname) <> ''
        GROUP BY data_id, surname
      )

      INSERT INTO eroll_surname (
        data_id,
        surname,
        v_count,
        r_count,
        castid_surname,
        caste,
        cast_cat,
        religion,
        district,
        block,
        ac_no,
        pc_no,
        process_status,
        process_count,
        last_process,
        processed_by,
        updated_at
      )
      SELECT
        a.data_id,
        a.surname,
        a.v_count,
        a.r_count,
        a.castid_surname,
        a.caste,
        a.cast_cat,
        a.religion,
        a.district,
        a.block,
        a.ac_no,
        a.pc_no,
        TRUE,
        1,
        CURRENT_TIMESTAMP,
        $${idx},
        CURRENT_TIMESTAMP
      FROM aggregated a
      ORDER BY a.data_id, a.surname
    `;

      values.push(processedBy);

      const insertResult = await client.query(syncSql, values);

      const countValues = [dataIds];
      let countSql = `
      SELECT COUNT(*)::INT AS total
      FROM eroll_surname
      WHERE data_id = ANY($1)
    `;

      if (acNo) {
        countSql += ` AND ac_no = $2`;
        countValues.push(acNo);
      }

      const countResult = await client.query(countSql, countValues);

      await client.query("COMMIT");

      return {
        success: true,
        insertedRows: insertResult.rowCount || 0,
        totalRows: countResult.rows[0]?.total || 0,
        processedDataIds: dataIds,
      };
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("syncSurname model error:", error);
      throw error;
    } finally {
      client.release();
    }
  },

  addEmptyRows: async (req) => {
    try {
      let { table, data_id, rowCount } = req.body;

      if (!table || !tableConfig[table]) {
        return {
          success: false,
          message: "Invalid table name"
        };
      }

      data_id = Number(data_id);
      rowCount = Number(rowCount);

      if (!data_id || isNaN(data_id)) {
        return {
          success: false,
          message: "Valid data_id is required"
        };
      }

      if (!rowCount || isNaN(rowCount) || rowCount < 1) {
        return {
          success: false,
          message: "Valid rowCount is required"
        };
      }

      if (rowCount > 100) {
        return {
          success: false,
          message: "Only 100 rows are allowed"
        };
      }

      // 1) Get current max id
      const maxIdResult = await pool.query(`
      SELECT COALESCE(MAX(id), 0) AS max_id
      FROM ${table}
    `);

      const maxId = Number(maxIdResult.rows[0]?.max_id || 0);

      // 2) Build insert values with next ids
      const values = [];
      const placeholders = [];

      for (let i = 0; i < rowCount; i++) {
        const nextId = maxId + i + 1;
        values.push(nextId, data_id);
        placeholders.push(`($${values.length - 1}, $${values.length})`);
      }

      const insertSql = `
      INSERT INTO ${table} (id, data_id)
      VALUES ${placeholders.join(", ")}
      RETURNING *;
    `;

      const result = await pool.query(insertSql, values);

      // 3) Sync sequence after manual id insert
      await pool.query(`
      SELECT setval(
        pg_get_serial_sequence('${table}', 'id'),
        (SELECT COALESCE(MAX(id), 1) FROM ${table}),
        true
      );
    `);

      return {
        success: true,
        message: `${result.rowCount} empty rows added successfully in ${table}`,
        insertedRows: result.rows,
        count: result.rowCount,
        start_id: maxId + 1,
        end_id: maxId + rowCount
      };
    } catch (error) {
      console.error("addEmptyRows error:", error);
      return {
        success: false,
        message: error.message || "Something went wrong"
      };
    }
  },

  saveMasterPatch: async (data) => {
    const client = await pool.connect();

    try {
      const { table, id, data_id, updates } = data;

      if (!table || !patchTableConfig[table]) {
        return {
          success: false,
          message: "Invalid table name"
        };
      }

      if (!id || isNaN(Number(id))) {
        return {
          success: false,
          message: "Valid id is required"
        };
      }

      if (data_id === undefined || data_id === null || data_id === "") {
        return {
          success: false,
          message: "data_id is required"
        };
      }

      if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
        return {
          success: false,
          message: "Valid updates object is required"
        };
      }

      const allowedColumns = patchTableConfig[table];
      const setClauses = [];
      const values = [];
      let index = 1;

      for (const [key, value] of Object.entries(updates)) {
        if (!allowedColumns.includes(key)) continue;
        if (key === "id" || key === "data_id") continue;

        let finalValue = value;

        if (key === "data_range" && finalValue && typeof finalValue === "object") {
          finalValue = JSON.stringify(finalValue);
        }

        if (key === "updated_at" && !finalValue) {
          finalValue = new Date();
        }

        setClauses.push(`${key} = $${index}`);
        values.push(finalValue === "" ? null : finalValue);
        index++;
      }

      if (
        (table === "dataid_importmaster" || table === "eroll_yojna_master") &&
        !Object.prototype.hasOwnProperty.call(updates, "updated_at")
      ) {
        setClauses.push(`updated_at = $${index}`);
        values.push(new Date());
        index++;
      }

      if (setClauses.length === 0) {
        return {
          success: false,
          message: "No valid fields provided to update"
        };
      }

      await client.query("BEGIN");

      values.push(Number(id));
      values.push(Number(data_id));

      const masterQuery = `
      UPDATE ${table}
      SET ${setClauses.join(", ")}
      WHERE id = $${index} AND data_id = $${index + 1}
      RETURNING *;
    `;

      const masterResult = await client.query(masterQuery, values);

      if (masterResult.rowCount === 0) {
        await client.query("ROLLBACK");
        return {
          success: false,
          message: "No matching row found"
        };
      }

      // only for dataid_importmaster => sync mapped fields
      if (table === "dataid_importmaster") {
        /**
         * dataid_importmaster -> eroll_mapping
         * ac_no       -> ac_id
         * ac_name_hi  -> ac_name (prefer hi)
         * ac_name_en  -> ac_name (fallback if hi not present)
         * is_active   -> is_active
         */
        const mappingSetClauses = [];
        const mappingValues = [];
        let mappingIdx = 1;

        if (Object.prototype.hasOwnProperty.call(updates, "ac_no")) {
          mappingSetClauses.push(`ac_id = $${mappingIdx}`);
          mappingValues.push(updates.ac_no === "" ? null : updates.ac_no);
          mappingIdx++;
        }

        if (Object.prototype.hasOwnProperty.call(updates, "ac_name_hi")) {
          mappingSetClauses.push(`ac_name = $${mappingIdx}`);
          mappingValues.push(updates.ac_name_hi === "" ? null : updates.ac_name_hi);
          mappingIdx++;
        } else if (Object.prototype.hasOwnProperty.call(updates, "ac_name_en")) {
          mappingSetClauses.push(`ac_name = $${mappingIdx}`);
          mappingValues.push(updates.ac_name_en === "" ? null : updates.ac_name_en);
          mappingIdx++;
        }

        if (Object.prototype.hasOwnProperty.call(updates, "is_active")) {
          mappingSetClauses.push(`is_active = $${mappingIdx}`);
          mappingValues.push(updates.is_active === "" ? null : updates.is_active);
          mappingIdx++;
        }

        if (mappingSetClauses.length > 0) {
          mappingValues.push(Number(data_id));

          const mappingQuery = `
          UPDATE eroll_mapping
          SET ${mappingSetClauses.join(", ")},
              updated_at = CURRENT_DATE,
              update_by = CURRENT_TIMESTAMP
          WHERE data_id = $${mappingIdx};
        `;

          await client.query(mappingQuery, mappingValues);
        }

        /**
         * dataid_importmaster -> eroll_db
         * ac_no -> ac_no
         */
        const dbSetClauses = [];
        const dbValues = [];
        let dbIdx = 1;

        if (Object.prototype.hasOwnProperty.call(updates, "ac_no")) {
          dbSetClauses.push(`ac_no = $${dbIdx}`);
          dbValues.push(updates.ac_no === "" ? null : updates.ac_no);
          dbIdx++;
        }

        if (dbSetClauses.length > 0) {
          dbValues.push(Number(data_id));

          const dbQuery = `
          UPDATE eroll_db
          SET ${dbSetClauses.join(", ")},
              update_by = CURRENT_TIMESTAMP
          WHERE data_id = $${dbIdx};
        `;

          await client.query(dbQuery, dbValues);
        }
      }

      await client.query("COMMIT");

      try {
        await refreshGlobalCache();
      } catch (cacheError) {
        console.error("Cache refresh failed:", cacheError);
      }

      return {
        success: true,
        message:
          table === "dataid_importmaster"
            ? "Row updated successfully and synced"
            : "Row updated successfully",
        data: masterResult.rows[0]
      };
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("saveMasterPatch error:", error);

      return {
        success: false,
        message: error.message || "Something went wrong"
      };
    } finally {
      client.release();
    }
  },

  getMasterForExcel: async (filters = {}) => {
    try {
      const { table } = filters;

      if (!table || !downloadTableConfig[table]) {
        throw new Error("Invalid table name");
      }

      const conditions = [];
      const values = [];
      let index = 1;

      if (filters.data_id !== undefined && filters.data_id !== "") {
        conditions.push(`data_id = $${index}`);
        values.push(
          table === "eroll_yojna_master" ? String(filters.data_id) : Number(filters.data_id)
        );
        index++;
      }

      if (
        filters.is_active !== undefined &&
        filters.is_active !== "" &&
        (table === "dataid_importmaster" || table === "eroll_yojna_master")
      ) {
        conditions.push(`is_active = $${index}`);
        values.push(Number(filters.is_active));
        index++;
      }

      if (
        filters.dropdown_name !== undefined &&
        filters.dropdown_name !== "" &&
        table === "eroll_dropdown"
      ) {
        conditions.push(`dropdown_name = $${index}`);
        values.push(filters.dropdown_name);
        index++;
      }

      if (
        filters.reg_name !== undefined &&
        filters.reg_name !== "" &&
        table === "eroll_yojna_master"
      ) {
        conditions.push(`reg_name = $${index}`);
        values.push(filters.reg_name);
        index++;
      }

      if (
        filters.value_id !== undefined &&
        filters.value_id !== "" &&
        table === "eroll_dropdown"
      ) {
        conditions.push(`value_id = $${index}`);
        values.push(filters.value_id);
        index++;
      }

      if (filters.search !== undefined && filters.search !== "") {
        if (table === "dataid_importmaster") {
          conditions.push(`(
          COALESCE(ac_name_hi, '') ILIKE $${index}
          OR COALESCE(ac_name_en, '') ILIKE $${index}
          OR COALESCE(pc_name_hi, '') ILIKE $${index}
          OR COALESCE(pc_name_en, '') ILIKE $${index}
          OR COALESCE(district_hi, '') ILIKE $${index}
          OR COALESCE(district_en, '') ILIKE $${index}
          OR COALESCE(party_district_hi, '') ILIKE $${index}
          OR COALESCE(party_district_en, '') ILIKE $${index}
          OR COALESCE(div_name_hi, '') ILIKE $${index}
          OR COALESCE(div_name_en, '') ILIKE $${index}
        )`);
          values.push(`%${filters.search}%`);
          index++;
        }

        if (table === "eroll_castmaster") {
          conditions.push(`(
          COALESCE(religion_en, '') ILIKE $${index}
          OR COALESCE(religion_hi, '') ILIKE $${index}
          OR COALESCE(castcat_en, '') ILIKE $${index}
          OR COALESCE(castcat_hi, '') ILIKE $${index}
          OR COALESCE(castida_en, '') ILIKE $${index}
          OR COALESCE(castida_hi, '') ILIKE $${index}
          OR COALESCE(castid, '') ILIKE $${index}
        )`);
          values.push(`%${filters.search}%`);
          index++;
        }

        if (table === "eroll_dropdown") {
          conditions.push(`(
          COALESCE(value_hi, '') ILIKE $${index}
          OR COALESCE(value_en, '') ILIKE $${index}
          OR COALESCE(dropdown_name, '') ILIKE $${index}
          OR COALESCE(value_id, '') ILIKE $${index}
        )`);
          values.push(`%${filters.search}%`);
          index++;
        }

        if (table === "eroll_yojna_master") {
          conditions.push(`(
          COALESCE(yojna_name, '') ILIKE $${index}
          OR COALESCE(reg_name, '') ILIKE $${index}
        )`);
          values.push(`%${filters.search}%`);
          index++;
        }
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const config = downloadTableConfig[table];
      const query = config.query({ whereClause });

      const result = await pool.query(query, values);

      const formattedRows = result.rows.map((row, idx) => ({
        S_NO: idx + 1,
        ...row
      }));

      return {
        rows: formattedRows,
        sheetName: config.sheetName
      };
    } catch (error) {
      console.log("getMasterForExcel error => ", error);
      throw error;
    }
  },

  importMasterCsv: async ({ table, fileBuffer, originalName }) => {
    const client = await pool.connect();

    try {
      const config = importCsvTableConfig[table];

      if (!config) {
        return {
          success: false,
          message: "Invalid table name"
        };
      }

      if (!originalName.toLowerCase().endsWith(".csv")) {
        return {
          success: false,
          message: "Only CSV file is allowed"
        };
      }

      const rawRows = await parseCsvBuffer(fileBuffer);

      if (!rawRows.length) {
        return {
          success: false,
          message: "CSV file is empty"
        };
      }

      const cleanedRows = rawRows
        .map((row) => normalizeImportedRow(row, config))
        .filter((row) => row.data_id !== null && row.data_id !== undefined && row.data_id !== "");

      if (!cleanedRows.length) {
        return {
          success: false,
          message: "No valid rows found in CSV"
        };
      }

      const uniqueDataIds = [
        ...new Set(
          (cleanedRows || [])
            .map((row) => row?.data_id)
            .filter((id) => id !== undefined && id !== null && id !== "")
        )
      ];

      await client.query("BEGIN");

      const deletePlaceholders = uniqueDataIds.map((_, i) => `$${i + 1}`).join(", ");
      const deleteQuery = `
      DELETE FROM ${config.tableName}
      WHERE data_id IN (${deletePlaceholders})
    `;

      await client.query(deleteQuery, uniqueDataIds);

      await bulkInsertRows(
        client,
        config.tableName,
        config.insertColumns,
        cleanedRows
      );

      await client.query("COMMIT");

      try {
        await refreshGlobalCache();
      } catch (cacheError) {
        console.error("Cache refresh failed:", cacheError);
      }

      return {
        success: true,
        message: `${cleanedRows.length} row(s) imported successfully into ${table}. Existing rows for imported data_id values were overridden.`,
        count: cleanedRows.length,
        overriddenDataIds: uniqueDataIds
      };
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("importMasterCsv error:", error);
      return {
        success: false,
        message: error.message || "Import failed"
      };
    } finally {
      client.release();
    }
  },

  getSurnameList: async (req) => {
    try {
      const body = req.body || {};

      const page = Math.max(parseInt(body.page, 10) || 1, 1);

      let limit =
        body.limit === "All"
          ? 100000
          : Math.max(parseInt(body.limit, 10) || 50, 1);

      if (limit > 100000) {
        limit = 100000;
      }

      const offset = (page - 1) * limit;

      const searchField = String(body.searchField || "All").trim();
      const searchValue = String(body.searchValue || "").trim();
      const minTotalCount =
        body.min_total_count !== undefined &&
          body.min_total_count !== null &&
          body.min_total_count !== ""
          ? Number(body.min_total_count)
          : null;

      const sortByMap = {
        id: "es.id",
        data_id: "es.data_id",
        surname: "es.surname",
        v_count: "es.v_count",
        r_count: "es.r_count",
        total_count: "(COALESCE(es.v_count, 0) + COALESCE(es.r_count, 0))",
        castid_surname: "es.castid_surname",
        caste: "es.caste",
        cast_cat: "es.cast_cat",
        religion: "es.religion",
        district: "es.district",
        block: "es.block",
        ac_no: "es.ac_no",
        pc_no: "es.pc_no",
        process_status: "es.process_status",
        process_count: "es.process_count",
        last_process: "es.last_process",
        processed_by: "es.processed_by",
        updated_at: "es.updated_at",
      };

      const sortBy =
        sortByMap[body.sortBy] ||
        "(COALESCE(es.v_count, 0) + COALESCE(es.r_count, 0))";

      const sortOrder =
        String(body.sortOrder || "DESC").toUpperCase() === "ASC" ? "ASC" : "DESC";

      const values = [];
      let idx = 1;
      const where = [];

      const dataIds = Array.isArray(body.data_id)
        ? body.data_id.map(Number).filter(Boolean)
        : body.data_id
          ? String(body.data_id)
            .split(",")
            .map(Number)
            .filter(Boolean)
          : [];

      if (dataIds.length) {
        where.push(`es.data_id = ANY($${idx}::int[])`);
        values.push(dataIds);
        idx++;
      }

      if (body.ac_no !== undefined && body.ac_no !== null && body.ac_no !== "") {
        where.push(`es.ac_no = $${idx}`);
        values.push(Number(body.ac_no));
        idx++;
      }

      if (body.pc_no !== undefined && body.pc_no !== null && body.pc_no !== "") {
        where.push(`es.pc_no = $${idx}`);
        values.push(Number(body.pc_no));
        idx++;
      }

      if (body.castid_surname) {
        where.push(`es.castid_surname ILIKE $${idx}`);
        values.push(`%${String(body.castid_surname).trim()}%`);
        idx++;
      }

      if (body.cast_cat) {
        where.push(`es.cast_cat ILIKE $${idx}`);
        values.push(`%${String(body.cast_cat).trim()}%`);
        idx++;
      }

      if (body.caste) {
        where.push(`es.caste ILIKE $${idx}`);
        values.push(`%${String(body.caste).trim()}%`);
        idx++;
      }

      if (body.religion) {
        where.push(`es.religion ILIKE $${idx}`);
        values.push(`%${String(body.religion).trim()}%`);
        idx++;
      }

      if (body.district) {
        where.push(`es.district ILIKE $${idx}`);
        values.push(`%${String(body.district).trim()}%`);
        idx++;
      }

      if (body.block) {
        where.push(`es.block ILIKE $${idx}`);
        values.push(`%${String(body.block).trim()}%`);
        idx++;
      }

      if (
        body.process_status !== undefined &&
        body.process_status !== null &&
        body.process_status !== ""
      ) {
        where.push(`es.process_status = $${idx}`);
        values.push(
          body.process_status === true ||
          body.process_status === "true" ||
          body.process_status === 1 ||
          body.process_status === "1"
        );
        idx++;
      }

      if (
        body.processed_by !== undefined &&
        body.processed_by !== null &&
        body.processed_by !== ""
      ) {
        where.push(`es.processed_by = $${idx}`);
        values.push(Number(body.processed_by));
        idx++;
      }

      if (searchValue) {
        if (searchField === "voter-surname") {
          where.push(`COALESCE(es.v_count, 0) > 0 AND es.surname ILIKE $${idx}`);
          values.push(`%${searchValue}%`);
          idx++;
        } else if (searchField === "relation-surname") {
          where.push(`COALESCE(es.r_count, 0) > 0 AND es.surname ILIKE $${idx}`);
          values.push(`%${searchValue}%`);
          idx++;
        } else {
          where.push(`es.surname ILIKE $${idx}`);
          values.push(`%${searchValue}%`);
          idx++;
        }
      } else {
        if (searchField === "voter-surname") {
          where.push(`COALESCE(es.v_count, 0) > 0`);
        } else if (searchField === "relation-surname") {
          where.push(`COALESCE(es.r_count, 0) > 0`);
        }
      }

      if (minTotalCount !== null && !Number.isNaN(minTotalCount)) {
        if (searchField === "voter-surname") {
          where.push(`COALESCE(es.v_count, 0) >= $${idx}`);
        } else if (searchField === "relation-surname") {
          where.push(`COALESCE(es.r_count, 0) >= $${idx}`);
        } else {
          where.push(`(COALESCE(es.v_count, 0) + COALESCE(es.r_count, 0)) >= $${idx}`);
        }
        values.push(minTotalCount);
        idx++;
      }

      const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

      const countSql = `
      SELECT COUNT(*)::INT AS total
      FROM eroll_surname es
      ${whereSql}
    `;

      // COALESCE(es.v_count, 0) + COALESCE(es.r_count, 0) AS total_count,

      const dataSql = `
      SELECT
        es.id,
        es.data_id,
        es.surname,
        es.v_count,
        es.r_count,
        es.castid_surname,
        es.caste,
        es.cast_cat,
        es.religion,
        es.district,
        es.block,
        es.ac_no,
        es.pc_no,
        es.process_status,
        es.process_count,
        CASE
          WHEN es.last_process IS NULL THEN NULL
          WHEN es.last_process::date = CURRENT_DATE THEN 'Today'
          WHEN es.last_process::date = CURRENT_DATE - INTERVAL '1 day' THEN 'Yesterday'
          ELSE TO_CHAR(es.last_process, 'DD Mon YYYY')
        END AS last_process,
        es.processed_by,
        CASE
          WHEN es.updated_at IS NULL THEN NULL
          WHEN es.updated_at::date = CURRENT_DATE THEN 'Today'
          WHEN es.updated_at::date = CURRENT_DATE - INTERVAL '1 day' THEN 'Yesterday'
          ELSE TO_CHAR(es.updated_at, 'DD Mon YYYY')
        END AS updated_at
      FROM eroll_surname es
      ${whereSql}
      ORDER BY v_count DESC
      LIMIT $${idx} OFFSET $${idx + 1}
    `;

      const countResult = await pool.query(countSql, values);
      const total = Number(countResult.rows[0]?.total || 0);

      const dataValues = [...values, limit, offset];
      const dataResult = await pool.query(dataSql, dataValues);

      const totalPages = total > 0 ? Math.ceil(total / limit) : 0;

      return {
        success: true,
        data: dataResult.rows || [],
        pagination: {
          total,
          page,
          limit,
          totalPages,
          hasNextPage: page < totalPages,
          hasPrevPage: page > 1,
        },
      };
    } catch (error) {
      console.error("getSurnameList model error:", error);
      throw error;
    }
  },

  syncDeleteDataId: async (data) => {
    const client = await pool.connect();

    try {
      let dataIds = data.data_ids || [];

      if (!Array.isArray(dataIds)) {
        dataIds = [dataIds];
      }

      dataIds = dataIds
        .map((id) => Number(id))
        .filter((id) => Number.isInteger(id) && id > 0);

      if (dataIds.length === 0) {
        return {
          success: false,
          message: "Valid data_id is required",
        };
      }

      await client.query("BEGIN");

      const inactiveImportMasterQuery = `
      UPDATE dataid_importmaster
      SET
        is_active = 0,
        updated_at = NOW()
      WHERE data_id = ANY($1::int[])
    `;
      await client.query(inactiveImportMasterQuery, [dataIds]);

      const deleteQueries = [
        {
          table: "eroll_castmaster",
          query: `DELETE FROM eroll_castmaster WHERE data_id = ANY($1::int[])`,
          params: [dataIds],
        },
        {
          table: "eroll_dropdown",
          query: `DELETE FROM eroll_dropdown WHERE data_id = ANY($1::int[])`,
          params: [dataIds],
        },
        {
          table: "eroll_yojna_master",
          query: `DELETE FROM eroll_yojna_master WHERE data_id = ANY($1::text[])`,
          params: [dataIds.map(String)],
        },
        {
          table: "user_data_assignments",
          query: `DELETE FROM user_data_assignments WHERE data_id = ANY($1::bigint[])`,
          params: [dataIds],
        },

        {
          table: "eroll_db",
          query: `DELETE FROM eroll_db WHERE data_id = ANY($1::int[])`,
          params: [dataIds],
        },
        {
          table: "eroll_mapping",
          query: `DELETE FROM eroll_mapping WHERE data_id = ANY($1::int[])`,
          params: [dataIds],
        },
        {
          table: "eroll_surname",
          query: `DELETE FROM eroll_surname WHERE data_id = ANY($1::int[])`,
          params: [dataIds],
        },
      ];

      for (const item of deleteQueries) {
        await client.query(item.query, item.params);
      }

      await client.query("COMMIT");

      return {
        success: true,
        message: "Data ID synced successfully. dataid_importmaster inactivated and related data deleted.",
        data: {
          data_ids: dataIds,
        },
      };
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("syncDeleteDataId error:", error);
      throw error;
    } finally {
      client.release();
    }
  },

  inactivateDataId: async (data) => {
    const client = await pool.connect();

    try {
      let dataIds = data?.data_id || data?.data_ids || [];

      // support single value also
      if (!Array.isArray(dataIds)) {
        dataIds = [dataIds];
      }

      // clean values
      dataIds = dataIds
        .map((id) => Number(id))
        .filter((id) => Number.isInteger(id) && id > 0);

      if (!dataIds.length) {
        throw new Error("Valid data_id is required");
      }

      await client.query("BEGIN");

      const checkQuery = `
        SELECT data_id
        FROM dataid_importmaster
        WHERE data_id = ANY($1::int[])
      `;
      const checkRes = await client.query(checkQuery, [dataIds]);

      if (checkRes.rowCount === 0) {
        throw new Error("No matching data_id found");
      }

      const foundIds = checkRes.rows.map((row) => Number(row.data_id));
      const missingIds = dataIds.filter((id) => !foundIds.includes(id));

      const updateQuery = `
        UPDATE dataid_importmaster
        SET
          is_active = 0,
          updated_at = NOW()
        WHERE data_id = ANY($1::int[])
      `;
      const updateRes = await client.query(updateQuery, [foundIds]);

      await client.query("COMMIT");

      return {
        success: true,
        message: "Data id inactivated successfully",
        updated_count: updateRes.rowCount,
        updated_data_ids: foundIds,
        missing_data_ids: missingIds
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  },

  getCountsByDataIds: async (dataIds = []) => {
    try {
      if (!Array.isArray(dataIds) || dataIds.length === 0) {
        return {
          success: false,
          message: "data_ids array is required",
          data: []
        };
      }

      const res = await pool.query(
        `
      SELECT 
        data_id,
        COUNT(*)::BIGINT AS total_count
      FROM eroll_db
      WHERE data_id = ANY($1)
      GROUP BY data_id
      `,
        [dataIds]
      );

      const dbCounts = res.rows;

      const map = new Map(
        dbCounts.map(row => [Number(row.data_id), Number(row.total_count)])
      );

      const finalCounts = dataIds.map(id => ({
        data_id: id,
        total_count: map.get(id) || 0
      }));

      if (finalCounts.length > 0) {
        const values = [];
        const placeholders = [];

        finalCounts.forEach((row, index) => {
          const i = index * 2;
          placeholders.push(`($${i + 1}, $${i + 2})`);
          values.push(row.data_id, row.total_count);
        });

        await pool.query(
          `
        INSERT INTO eroll_db_counts 
          (data_id, total_count)
        VALUES 
          ${placeholders.join(", ")}
        ON CONFLICT (data_id)
        DO UPDATE SET
          total_count = EXCLUDED.total_count,
          updated_at = NOW()
        `,
          values
        );
      }

      return {
        success: true,
        message: "Counts calculated and stored successfully",
        data: finalCounts
      };

    } catch (error) {
      return {
        success: false,
        message: "Failed to calculate/store counts",
        error: error.message
      };
    }
  }

}
