const { pool } = require("../config/config");

const TABLE_MAP = {
  eroll_db: 'eroll_db',
  voter_list: 'eroll_db'
};

const WISE_MAP = {
  village: {
    idColumn: 'village_id',
    nameColumn: 'village'
  },
  gp_ward: {
    idColumn: 'gp_ward_id',
    nameColumn: 'gp_ward'
  },
  block: {
    idColumn: 'block_id',
    nameColumn: 'block'
  },
  psb: {
    idColumn: 'psb_id',
    nameColumn: 'psb'
  },
  coordinate: {
    idColumn: 'coordinate_id',
    nameColumn: 'coordinate'
  },
  kendra: {
    idColumn: 'kendra_id',
    nameColumn: 'kendra'
  },
  mandal: {
    idColumn: 'mandal_id',
    nameColumn: 'mandal'
  },
  pjila: {
    idColumn: 'pjila_id',
    nameColumn: 'pjila'
  },
  pincode: {
    idColumn: 'pincode_id',
    nameColumn: 'pincode'
  },
  postoff: {
    idColumn: 'postoff_id',
    nameColumn: 'postoff'
  },
  policst: {
    idColumn: 'policst_id',
    nameColumn: 'policst'
  },
  ac: {
    idColumn: 'ac_id',
    nameColumn: 'ac_name'
  },
  bhag: {
    idColumn: 'bhag_no',
    nameColumn: 'bhag_no'
  },
  section: {
    idColumn: 'sec_no',
    nameColumn: 'section'
  }
};

module.exports = {

  getMeta: async () => {
    const activeDataQuery = `
      SELECT
        data_id,
        data_id_name_en,
        data_id_name_hi,
        ac_no,
        ac_name_en,
        ac_name_hi,
        is_active,
        data_range,
        updated_at
      FROM dataid_importmaster
      WHERE is_active = true
      ORDER BY updated_at DESC, data_id DESC;
    `;

    const { rows: datasets } = await pool.query(activeDataQuery);

    return {
      tables: [
        {
          label: 'Voter List',
          value: 'voter_list'
        }
      ],
      wise_types: [
        { label: 'All', value: 'all' },
        { label: 'AC', value: 'ac' },
        { label: 'Bhag', value: 'bhag' },
        { label: 'Section', value: 'section' },
        { label: 'Village', value: 'village' },
        { label: 'GP Ward', value: 'gp_ward' },
        { label: 'Block', value: 'block' },
        { label: 'PSB', value: 'psb' },
        { label: 'Coordinate', value: 'coordinate' },
        { label: 'Kendra', value: 'kendra' },
        { label: 'Mandal', value: 'mandal' },
        { label: 'Pjila', value: 'pjila' },
        { label: 'Pincode', value: 'pincode' },
        { label: 'Post Office', value: 'postoff' },
        { label: 'Police Station', value: 'policst' }
      ],
      datasets
    };
  },

  getOptions: async ({
    table,
    wise_type,
    data_id,
    data_ids,
    ['data_ids[]']: data_ids_bracket,
    block_id,
    gp_ward_id,
    ac_id,
    bhag_no,
    mandal_id
  } = {}) => {
    try {
      const selectedTable = TABLE_MAP[table];
      if (!selectedTable) {
        throw new Error('Invalid table selected');
      }

      const cleanValue = (value) => {
        if (
          value === undefined ||
          value === null ||
          value === '' ||
          value === 'null' ||
          value === 'undefined'
        ) return null;
        return String(value).trim();
      };

      const toIntOrNull = (value) => {
        const cleaned = cleanValue(value);
        if (cleaned === null) return null;
        const num = Number(cleaned);
        return Number.isNaN(num) ? null : num;
      };

      const toIntArray = (value) => {
        if (value === undefined || value === null || value === '') return [];

        if (Array.isArray(value)) {
          return value
            .map((v) => Number(v))
            .filter((v) => !Number.isNaN(v));
        }

        if (typeof value === 'string') {
          return value
            .split(',')
            .map((v) => Number(String(v).trim()))
            .filter((v) => !Number.isNaN(v));
        }

        const one = Number(value);
        return Number.isNaN(one) ? [] : [one];
      };

      const cleanedTable = cleanValue(table);
      const cleanedWiseType = cleanValue(wise_type);
      const cleanedBlockId = cleanValue(block_id);
      const cleanedGpWardId = cleanValue(gp_ward_id);
      const cleanedAcId = toIntOrNull(ac_id);
      const cleanedBhagNo = toIntOrNull(bhag_no);
      const cleanedMandalId = cleanValue(mandal_id);

      const rawDataIds =
        data_ids !== undefined && data_ids !== null && data_ids !== ''
          ? data_ids
          : data_ids_bracket !== undefined && data_ids_bracket !== null && data_ids_bracket !== ''
            ? data_ids_bracket
            : data_id;

      const cleanedDataIds = toIntArray(rawDataIds);

      const dataIdsQuery = `
      SELECT DISTINCT
        data_id,
        data_id_name_hi,
        data_id_name_en
      FROM dataid_importmaster
      WHERE COALESCE(is_active, 0) = 1
      ORDER BY data_id DESC;
    `;

      const dataIdsResult = await pool.query(dataIdsQuery);

      if (!cleanedDataIds.length) {
        return {
          success: true,
          message: 'Please select data id first',
          step: 'dataid',
          table: cleanedTable,
          db_table: selectedTable,
          wise_type: cleanedWiseType,
          data_ids: dataIdsResult.rows,
          options: [],
          cast_options: []
        };
      }

      const selectedDataIdsResult = await pool.query(
        `
      SELECT
        data_id,
        data_id_name_hi,
        data_id_name_en
      FROM dataid_importmaster
      WHERE data_id = ANY($1::int[])
        AND COALESCE(is_active, 0) = 1
      ORDER BY data_id DESC;
      `,
        [cleanedDataIds]
      );

      if (!selectedDataIdsResult.rows.length) {
        throw new Error('Invalid or inactive data_id/data_ids');
      }

      const baseResponse = {
        success: true,
        table: cleanedTable,
        db_table: selectedTable,
        wise_type: cleanedWiseType,
        selected_data_ids: cleanedDataIds,
        selected_data_id_details: selectedDataIdsResult.rows
      };

      const getCastOptions = async ({
        blockId = null,
        gpWardId = null,
        acId = null,
        bhagNo = null,
        mandalId = null
      } = {}) => {
        const params = [cleanedDataIds];
        let idx = 2;

        let joinClause = '';
        const where = [
          `ed.data_id = ANY($1::int[])`,
          `ed.castid IS NOT NULL`,
          `TRIM(ed.castid) <> ''`
        ];

        const needsMappingJoin =
          !!blockId ||
          !!gpWardId ||
          acId !== null ||
          bhagNo !== null ||
          !!mandalId;

        if (needsMappingJoin) {
          joinClause = `
          LEFT JOIN eroll_mapping em
            ON em.data_id = ed.data_id
           AND em.ac_id = ed.ac_no
           AND em.bhag_no = ed.bhag_no
           AND em.sec_no = ed.sec_no
        `;

          if (blockId) {
            where.push(`em.block_id = $${idx++}`);
            params.push(blockId);
          }

          if (gpWardId) {
            where.push(`em.gp_ward_id = $${idx++}`);
            params.push(gpWardId);
          }

          if (acId !== null) {
            where.push(`em.ac_id = $${idx++}`);
            params.push(acId);
          }

          if (bhagNo !== null) {
            where.push(`em.bhag_no = $${idx++}`);
            params.push(bhagNo);
          }

          if (mandalId) {
            where.push(`em.mandal_id = $${idx++}`);
            params.push(mandalId);
          }
        }

        const castQuery = `
        SELECT DISTINCT
          UPPER(TRIM(ed.castid)) AS id,
          UPPER(TRIM(ed.castid)) AS name
        FROM eroll_db ed
        ${joinClause}
        WHERE ${where.join(' AND ')}
        ORDER BY UPPER(TRIM(ed.castid)) ASC;
      `;

        const castResult = await pool.query(castQuery, params);
        return castResult.rows || [];
      };

      if (cleanedWiseType === 'dataid') {
        return {
          ...baseResponse,
          message: 'Data ids fetched successfully',
          step: 'dataid',
          options: selectedDataIdsResult.rows.map((row) => ({
            id: String(row.data_id),
            name: row.data_id_name_hi || row.data_id_name_en || String(row.data_id)
          })),
          cast_options: await getCastOptions()
        };
      }

      // block first step for block / gp_ward / gram
      if (['block', 'gp_ward', 'gram'].includes(cleanedWiseType) && !cleanedBlockId) {
        const blockResult = await pool.query(
          `
        SELECT DISTINCT
          em.block_id AS id,
          em.block AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.block_id IS NOT NULL
          AND em.block IS NOT NULL
          AND TRIM(em.block::text) <> ''
        ORDER BY em.block ASC;
        `,
          [cleanedDataIds]
        );

        return {
          ...baseResponse,
          message: 'Block options fetched successfully',
          step: 'block',
          options: blockResult.rows,
          cast_options: await getCastOptions()
        };
      }

      if (cleanedWiseType === 'block') {
        return {
          ...baseResponse,
          message: 'Block options fetched successfully',
          step: 'block',
          options: [],
          cast_options: await getCastOptions({ blockId: cleanedBlockId })
        };
      }

      if (cleanedWiseType === 'gp_ward') {
        const gpResult = await pool.query(
          `
        SELECT DISTINCT
          em.gp_ward_id AS id,
          em.gp_ward AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.block_id = $2
          AND em.gp_ward_id IS NOT NULL
          AND em.gp_ward IS NOT NULL
          AND TRIM(em.gp_ward::text) <> ''
        ORDER BY em.gp_ward ASC;
        `,
          [cleanedDataIds, cleanedBlockId]
        );

        return {
          ...baseResponse,
          message: 'GP ward fetched successfully',
          step: 'gp_ward',
          selected_block_id: cleanedBlockId,
          options: gpResult.rows,
          cast_options: await getCastOptions({ blockId: cleanedBlockId })
        };
      }

      if (cleanedWiseType === 'gram') {
        if (!cleanedGpWardId) {
          const gpResult = await pool.query(
            `
          SELECT DISTINCT
            em.gp_ward_id AS id,
            em.gp_ward AS name
          FROM eroll_mapping em
          WHERE em.data_id = ANY($1::int[])
            AND em.block_id = $2
            AND em.gp_ward_id IS NOT NULL
            AND em.gp_ward IS NOT NULL
            AND TRIM(em.gp_ward::text) <> ''
          ORDER BY em.gp_ward ASC;
          `,
            [cleanedDataIds, cleanedBlockId]
          );

          return {
            ...baseResponse,
            message: 'GP ward options fetched successfully',
            step: 'gp_ward',
            selected_block_id: cleanedBlockId,
            options: gpResult.rows,
            cast_options: await getCastOptions({ blockId: cleanedBlockId })
          };
        }

        const villageResult = await pool.query(
          `
        SELECT DISTINCT
          em.village_id AS id,
          em.village AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.block_id = $2
          AND em.gp_ward_id = $3
          AND em.village_id IS NOT NULL
          AND em.village IS NOT NULL
          AND TRIM(em.village::text) <> ''
        ORDER BY em.village ASC;
        `,
          [cleanedDataIds, cleanedBlockId, cleanedGpWardId]
        );

        return {
          ...baseResponse,
          message: 'Village fetched successfully',
          step: 'gram',
          selected_block_id: cleanedBlockId,
          selected_gp_ward_id: cleanedGpWardId,
          options: villageResult.rows,
          cast_options: await getCastOptions({
            blockId: cleanedBlockId,
            gpWardId: cleanedGpWardId
          })
        };
      }

      if (cleanedWiseType === 'ac') {
        const acResult = await pool.query(
          `
        SELECT DISTINCT
          em.ac_id AS id,
          em.ac_name AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.ac_id IS NOT NULL
          AND em.ac_name IS NOT NULL
          AND TRIM(em.ac_name::text) <> ''
        ORDER BY em.ac_name ASC;
        `,
          [cleanedDataIds]
        );

        return {
          ...baseResponse,
          message: 'AC options fetched successfully',
          step: 'ac',
          options: acResult.rows,
          cast_options: await getCastOptions()
        };
      }

      if (cleanedWiseType === 'bhag') {
        const bhagResult = await pool.query(
          `
        SELECT DISTINCT
          em.bhag_no AS id,
          COALESCE(NULLIF(TRIM(em.bhag::text), ''), em.bhag_no::text) AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.bhag_no IS NOT NULL
        ORDER BY em.bhag_no ASC;
        `,
          [cleanedDataIds]
        );

        return {
          ...baseResponse,
          message: 'Bhag options fetched successfully',
          step: 'bhag',
          options: bhagResult.rows,
          cast_options: await getCastOptions({ bhagNo: cleanedBhagNo })
        };
      }

      if (cleanedWiseType === 'section') {
        if (cleanedBhagNo === null) {
          const bhagResult = await pool.query(
            `
          SELECT DISTINCT
            em.bhag_no AS id,
            COALESCE(NULLIF(TRIM(em.bhag::text), ''), em.bhag_no::text) AS name
          FROM eroll_mapping em
          WHERE em.data_id = ANY($1::int[])
            AND em.bhag_no IS NOT NULL
          ORDER BY em.bhag_no ASC;
          `,
            [cleanedDataIds]
          );

          return {
            ...baseResponse,
            message: 'Bhag options fetched successfully',
            step: 'bhag',
            options: bhagResult.rows,
            cast_options: await getCastOptions()
          };
        }

        const sectionResult = await pool.query(
          `
        SELECT DISTINCT
          em.sec_no AS id,
          COALESCE(NULLIF(TRIM(em.section::text), ''), em.sec_no::text) AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.bhag_no = $2
          AND em.sec_no IS NOT NULL
        ORDER BY em.sec_no ASC;
        `,
          [cleanedDataIds, cleanedBhagNo]
        );

        return {
          ...baseResponse,
          message: 'Section options fetched successfully',
          step: 'section',
          selected_bhag_no: cleanedBhagNo,
          options: sectionResult.rows,
          cast_options: await getCastOptions({ bhagNo: cleanedBhagNo })
        };
      }

      if (cleanedWiseType === 'mandal') {
        const mandalResult = await pool.query(
          `
        SELECT DISTINCT
          em.mandal_id AS id,
          em.mandal AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.mandal_id IS NOT NULL
          AND em.mandal IS NOT NULL
          AND TRIM(em.mandal::text) <> ''
        ORDER BY em.mandal ASC;
        `,
          [cleanedDataIds]
        );

        return {
          ...baseResponse,
          message: 'Mandal options fetched successfully',
          step: 'mandal',
          options: mandalResult.rows,
          cast_options: await getCastOptions({ mandalId: cleanedMandalId })
        };
      }

      if (cleanedWiseType === 'kendra') {
        if (!cleanedMandalId) {
          const mandalResult = await pool.query(
            `
          SELECT DISTINCT
            em.mandal_id AS id,
            em.mandal AS name
          FROM eroll_mapping em
          WHERE em.data_id = ANY($1::int[])
            AND em.mandal_id IS NOT NULL
            AND em.mandal IS NOT NULL
            AND TRIM(em.mandal::text) <> ''
          ORDER BY em.mandal ASC;
          `,
            [cleanedDataIds]
          );

          return {
            ...baseResponse,
            message: 'Mandal options fetched successfully',
            step: 'mandal',
            options: mandalResult.rows,
            cast_options: await getCastOptions()
          };
        }

        const kendraResult = await pool.query(
          `
        SELECT DISTINCT
          em.kendra_id AS id,
          em.kendra AS name
        FROM eroll_mapping em
        WHERE em.data_id = ANY($1::int[])
          AND em.mandal_id = $2
          AND em.kendra_id IS NOT NULL
          AND em.kendra IS NOT NULL
          AND TRIM(em.kendra::text) <> ''
        ORDER BY em.kendra ASC;
        `,
          [cleanedDataIds, cleanedMandalId]
        );

        return {
          ...baseResponse,
          message: 'Kendra options fetched successfully',
          step: 'kendra',
          selected_mandal_id: cleanedMandalId,
          options: kendraResult.rows,
          cast_options: await getCastOptions({ mandalId: cleanedMandalId })
        };
      }

      throw new Error('Unsupported wise_type selected');
    } catch (error) {
      console.log('[getOptions] ERROR =>', error);
      throw error;
    }
  },

  getUserAssignments: async (user_id, requesterRole, selected_role_id, currentUserId) => {
    try {
      const rolesSql = `
        SELECT
          id,
          name,
          code
        FROM roles
        WHERE is_active = true
        ORDER BY name ASC
      `;

      const isAdmin = ["admin", "super_admin"].includes(
        String(requesterRole || "").toLowerCase()
      );

      let allUsersSql = "";
      let allUsersParams = [];

      if (isAdmin) {
        allUsersSql = `
          SELECT
            u.id,
            u.username,
            u.mobile_no,
            u.role,
            u.modules_code,
            u.permission_code,
            CASE
              WHEN
                COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false)
                OR
                COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false)
              THEN true
              ELSE false
            END AS status
          FROM users u
          WHERE
            u.id != $2
            AND (
              (
                COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false) = true
                OR
                COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false) = true
              )
              OR
              u.modules_code IS NOT NULL
              OR
              u.permission_code IS NOT NULL
            )
            AND ($1::int IS NULL OR u.role_id = $1)
          ORDER BY u.id DESC
        `;
        allUsersParams = [
          selected_role_id ? Number(selected_role_id) : null,
          Number(currentUserId),
        ];
      } else {
        allUsersSql = `
          SELECT
            u.id,
            u.username,
            u.mobile_no,
            u.role,
            CASE
              WHEN
                COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false)
                OR
                COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false)
              THEN true
              ELSE false
            END AS status
          FROM users u
          WHERE
            u.id != $2
            AND (
              COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'web')::boolean, false) = true
              OR
              COALESCE((COALESCE(u.login_access, '{}')::jsonb ->> 'mobile')::boolean, false) = true
            )
            AND ($1::int IS NULL OR u.role_id = $1)
          ORDER BY u.id DESC
        `;
        allUsersParams = [
          selected_role_id ? Number(selected_role_id) : null,
          Number(currentUserId),
        ];
      }

      // jab user_id na ho tab sirf roles + users return karo
      if (!user_id) {
        const [allUsersResult, rolesResult] = await Promise.all([
          pool.query(allUsersSql, allUsersParams),
          pool.query(rolesSql),
        ]);

        return {
          modules_code: null,
          permission_code: null,
          data_assignments: [],
          column_permissions: [],
          all_users: allUsersResult.rows || [],
          roles: rolesResult.rows || [],
        };
      }

      const dataAssignmentsSql = `
        SELECT
          uda.id,
          uda.user_id,
          uda.db_table,
          uda.wise_type,
          uda.district,
          uda.ac,
          uda.pc,
          uda.party_jila,
          uda.data_id,
          uda.block_id,
          uda.gp_ward_id,
          uda.village_id,
          uda.ac_id,
          uda.bhag_no,
          uda.sec_no,
          uda.mandal_id,
          uda.kendra_id,
          uda.created_by,
          uda.updated_by,
          uda.created_at,
          uda.updated_at,
          uda.is_active,
          uda.age_from,
          uda.age_to,
          uda.cast_filter AS cast,

          dm.data_id_name_hi,
          COALESCE(dm.data_id_name_hi, 'N/A') AS data_id_label,

          acmap.ac_names,
          blockmap.block_names,
          gpmap.gp_ward_names,
          villagemap.village_names,
          mandalmap.mandal_names,
          kendramap.kendra_names,
          bhagmap.bhag_names,
          secmap.section_names

        FROM user_data_assignments uda

        LEFT JOIN dataid_importmaster dm
          ON dm.data_id = uda.data_id::integer

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.ac_name, ', ' ORDER BY x.sort_id) AS ac_names
          FROM (
            SELECT DISTINCT ON (em.ac_id)
              em.id AS sort_id,
              COALESCE(em.ac_name, 'N/A') AS ac_name
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.ac_id::text = ANY(regexp_split_to_array(COALESCE(uda.ac_id, ''), '\\s*,\\s*'))
            ORDER BY em.ac_id, em.id
          ) x
        ) acmap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.block, ', ' ORDER BY x.sort_id) AS block_names
          FROM (
            SELECT DISTINCT ON (em.block_id)
              em.id AS sort_id,
              COALESCE(em.block, 'N/A') AS block
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.block_id::text = ANY(regexp_split_to_array(COALESCE(uda.block_id, ''), '\\s*,\\s*'))
            ORDER BY em.block_id, em.id
          ) x
        ) blockmap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.gp_ward, ', ' ORDER BY x.sort_id) AS gp_ward_names
          FROM (
            SELECT DISTINCT ON (em.gp_ward_id)
              em.id AS sort_id,
              COALESCE(em.gp_ward, 'N/A') AS gp_ward
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.gp_ward_id::text = ANY(regexp_split_to_array(COALESCE(uda.gp_ward_id, ''), '\\s*,\\s*'))
            ORDER BY em.gp_ward_id, em.id
          ) x
        ) gpmap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.village, ', ' ORDER BY x.sort_id) AS village_names
          FROM (
            SELECT DISTINCT ON (em.village_id)
              em.id AS sort_id,
              COALESCE(em.village, 'N/A') AS village
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.village_id::text = ANY(regexp_split_to_array(COALESCE(uda.village_id, ''), '\\s*,\\s*'))
            ORDER BY em.village_id, em.id
          ) x
        ) villagemap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.mandal, ', ' ORDER BY x.sort_id) AS mandal_names
          FROM (
            SELECT DISTINCT ON (em.mandal_id)
              em.id AS sort_id,
              COALESCE(em.mandal, 'N/A') AS mandal
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.mandal_id::text = ANY(regexp_split_to_array(COALESCE(uda.mandal_id, ''), '\\s*,\\s*'))
            ORDER BY em.mandal_id, em.id
          ) x
        ) mandalmap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.kendra, ', ' ORDER BY x.sort_id) AS kendra_names
          FROM (
            SELECT DISTINCT ON (em.kendra_id)
              em.id AS sort_id,
              COALESCE(em.kendra, 'N/A') AS kendra
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.kendra_id::text = ANY(regexp_split_to_array(COALESCE(uda.kendra_id, ''), '\\s*,\\s*'))
            ORDER BY em.kendra_id, em.id
          ) x
        ) kendramap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.bhag, ', ' ORDER BY x.sort_id) AS bhag_names
          FROM (
            SELECT DISTINCT ON (em.bhag_no)
              em.id AS sort_id,
              COALESCE(em.bhag, 'N/A') AS bhag
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.bhag_no::text = ANY(regexp_split_to_array(COALESCE(uda.bhag_no, ''), '\\s*,\\s*'))
            ORDER BY em.bhag_no, em.id
          ) x
        ) bhagmap ON TRUE

        LEFT JOIN LATERAL (
          SELECT STRING_AGG(x.section, ', ' ORDER BY x.sort_id) AS section_names
          FROM (
            SELECT DISTINCT ON (em.sec_no)
              em.id AS sort_id,
              COALESCE(em.section, 'N/A') AS section
            FROM eroll_mapping em
            WHERE em.data_id = uda.data_id::integer
              AND em.sec_no::text = ANY(regexp_split_to_array(COALESCE(uda.sec_no, ''), '\\s*,\\s*'))
            ORDER BY em.sec_no, em.id
          ) x
        ) secmap ON TRUE

        WHERE uda.user_id = $1
          AND COALESCE(uda.is_active, 1) = 1
        ORDER BY uda.id DESC
      `;

      const columnPermissionsSql = `
        SELECT
          ucp.id,
          ucp.user_id,
          ucp.db_table,
          ucp.column_name,
          ucp.can_view,
          ucp.can_mask,
          ucp.can_edit,
          ucp.can_copy,
          ucp.created_by,
          ucp.updated_by,
          ucp.created_at,
          ucp.updated_at,
          ucp.assignment_id
        FROM user_column_permissions ucp
        WHERE ucp.user_id = $1
        ORDER BY ucp.id DESC
      `;

      const userInfoSql = `
        SELECT
          id,
          modules_code,
          permission_code
        FROM users
        WHERE id = $1
        LIMIT 1
      `;

      const [
        dataAssignmentsResult,
        columnPermissionsResult,
        userInfoResult,
        allUsersResult,
        rolesResult,
      ] = await Promise.all([
        pool.query(dataAssignmentsSql, [user_id]),
        pool.query(columnPermissionsSql, [user_id]),
        pool.query(userInfoSql, [user_id]),
        pool.query(allUsersSql, allUsersParams),
        pool.query(rolesSql),
      ]);

      const userInfo = userInfoResult.rows[0] || null;

      return {
        modules_code: userInfo?.modules_code || null,
        permission_code: userInfo?.permission_code || null,
        data_assignments: dataAssignmentsResult.rows || [],
        column_permissions: columnPermissionsResult.rows || [],
        all_users: allUsersResult.rows || [],
        roles: rolesResult.rows || [],
      };
    } catch (error) {
      console.error("getUserAssignments model error:", error);
      throw error;
    }
  },

  getPreview: async ({ table, wise_type, data_id, wise_value_id }) => {
    const selectedTable = TABLE_MAP[table];

    if (!selectedTable) {
      throw new Error('Invalid table selected');
    }

    if (!WISE_MAP.hasOwnProperty(wise_type)) {
      throw new Error('Invalid wise_type selected');
    }

    if (wise_type === 'all') {
      const countQuery = `
        SELECT COUNT(*)::int AS total_count
        FROM ${selectedTable}
        WHERE data_id = $1;
      `;

      const listQuery = `
        SELECT
          id,
          data_id,
          ac_no,
          bhag_no,
          sec_no,
          section,
          epic,
          vsno,
          vname,
          sex,
          age,
          relation,
          rname,
          hno,
          phone1,
          phone2,
          familyid,
          hof
        FROM ${selectedTable}
        WHERE data_id = $1
        ORDER BY id DESC
        LIMIT 100;
      `;

      const countResult = await pool.query(countQuery, [data_id]);
      const listResult = await pool.query(listQuery, [data_id]);

      return {
        total_count: countResult.rows[0]?.total_count || 0,
        rows: listResult.rows
      };
    }

    const { idColumn } = WISE_MAP[wise_type];

    if (!wise_value_id) {
      throw new Error('wise_value_id is required');
    }

    const countQuery = `
      SELECT COUNT(*)::int AS total_count
      FROM ${selectedTable} e
      INNER JOIN eroll_mapping m
        ON e.data_id = m.data_id
       AND e.ac_no = m.ac_id
       AND e.bhag_no = m.bhag_no
       AND e.sec_no = m.sec_no
      WHERE e.data_id = $1
        AND m.${idColumn} = $2;
    `;

    const listQuery = `
      SELECT
        e.id,
        e.data_id,
        e.ac_no,
        e.bhag_no,
        e.sec_no,
        e.section,
        e.epic,
        e.vsno,
        e.vname,
        e.sex,
        e.age,
        e.relation,
        e.rname,
        e.hno,
        e.phone1,
        e.phone2,
        e.familyid,
        e.hof
      FROM ${selectedTable} e
      INNER JOIN eroll_mapping m
        ON e.data_id = m.data_id
       AND e.ac_no = m.ac_id
       AND e.bhag_no = m.bhag_no
       AND e.sec_no = m.sec_no
      WHERE e.data_id = $1
        AND m.${idColumn} = $2
      ORDER BY e.id DESC
      LIMIT 100;
    `;

    const countResult = await pool.query(countQuery, [data_id, wise_value_id]);
    const listResult = await pool.query(listQuery, [data_id, wise_value_id]);

    return {
      total_count: countResult.rows[0]?.total_count || 0,
      rows: listResult.rows
    };
  },

  saveAssignments: async ({ user_id, data_assignments, column_permissions, updated_by }) => {
    const client = await pool.connect();

    const generateSixDigitCode = () => {
      return Math.floor(100000 + Math.random() * 900000);
    };

    const generateUniquePermissionCode = async (client, user_id) => {
      let permissionCode = null;
      let isUnique = false;
      let attempts = 0;

      while (!isUnique && attempts < 20) {
        permissionCode = generateSixDigitCode();

        const checkRes = await client.query(
          `
        SELECT id
        FROM users
        WHERE permission_code = $1
          AND id <> $2
        LIMIT 1
        `,
          [permissionCode, user_id]
        );

        if (checkRes.rows.length === 0) {
          isUnique = true;
        }

        attempts++;
      }

      if (!isUnique) {
        throw new Error('Unable to generate unique permission code. Please try again.');
      }

      return permissionCode;
    };

    try {
      await client.query('BEGIN');

      // Delete existing assignments and permissions
      await client.query(
        `DELETE FROM user_column_permissions WHERE user_id = $1`,
        [user_id]
      );

      await client.query(
        `DELETE FROM user_data_assignments WHERE user_id = $1`,
        [user_id]
      );

      const assignmentIdMap = {};

      // Insert all data assignments
      for (const item of data_assignments) {
        const insertAssignmentQuery = `
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
          age_from,
          age_to,
          cast_filter,
          created_by,
          updated_by,
          is_active
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, 1
        )
        RETURNING id
      `;

        const assignmentRes = await client.query(insertAssignmentQuery, [
          user_id,
          item.db_table || null,
          item.wise_type || null,
          item.district || null,
          item.ac || null,
          item.pc || null,
          item.party_jila || null,
          item.data_id || null,
          item.block_id || null,
          item.gp_ward_id || null,
          item.village_id || null,
          item.ac_id || null,
          item.bhag_no || null,
          item.sec_no || null,
          item.mandal_id || null,
          item.kendra_id || null,
          item.age_from ? parseInt(item.age_from) : null,
          item.age_to ? parseInt(item.age_to) : null,
          item.cast_filter || null,
          updated_by,
          updated_by
        ]);

        const assignmentId = assignmentRes.rows[0].id;

        if (item.temp_id) {
          assignmentIdMap[item.temp_id] = assignmentId;
        }
      }

      // Insert column permissions
      for (const col of column_permissions) {
        const assignment_id = col.temp_id ? assignmentIdMap[col.temp_id] : null;

        let can_view = !!col.can_view;
        const can_mask = !!col.can_mask;
        const can_edit = !!col.can_edit;
        const can_copy = !!col.can_copy;

        // Validation: mask and edit cannot both be true
        if (can_mask && can_edit) {
          throw new Error(`Column '${col.column_name}' cannot have both mask and edit permission`);
        }

        // If edit or mask is true, view must be true
        if ((can_edit || can_mask) && !can_view) {
          can_view = true;
        }

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
          updated_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (user_id, assignment_id, db_table, column_name)
        DO UPDATE SET
          can_view = EXCLUDED.can_view,
          can_mask = EXCLUDED.can_mask,
          can_edit = EXCLUDED.can_edit,
          can_copy = EXCLUDED.can_copy,
          updated_by = EXCLUDED.updated_by,
          updated_at = CURRENT_TIMESTAMP
        `,
          [
            user_id,
            assignment_id,
            col.db_table,
            col.column_name,
            can_view ? 1 : 0,
            can_mask ? 1 : 0,
            can_edit ? 1 : 0,
            can_copy ? 1 : 0,
            updated_by,
            updated_by
          ]
        );
      }

      // Generate new unique 6-digit permission code after permission assignment
      const permissionCode = await generateUniquePermissionCode(client, user_id);

      // Save permission code in users table
      await client.query(
        `
      UPDATE users
      SET
        permission_code = $1,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = $2
      `,
        [permissionCode, user_id]
      );

      // Fetch user's module code + permission code
      const userRes = await client.query(
        `
      SELECT
        id,
        modules_code,
        permission_code
      FROM users
      WHERE id = $1
      LIMIT 1
      `,
        [user_id]
      );

      await client.query('COMMIT');

      return {
        user_id,
        modules_code: userRes.rows[0]?.modules_code || null,
        permission_code: userRes.rows[0]?.permission_code || null,
        saved_data_assignments: data_assignments.length,
        saved_column_permissions: column_permissions.length
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  },

  applyAccessCodesToUser: async ({ user_id, modules_code, permission_code, updated_by }) => {
    const client = await pool.connect();

    const copyUserPermissions = async (targetUserId, sourceUserId) => {
      await client.query(
        `DELETE FROM user_permissions WHERE user_id = $1`,
        [targetUserId]
      );

      await client.query(
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
      `,
        [targetUserId, sourceUserId]
      );
    };

    const copyAssignmentsAndColumnPermissions = async (targetUserId, sourceUserId, actorId) => {
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
            actorId,
            actorId,
            item.is_active ?? 1,
            item.age_from,
            item.age_to,
            item.cast_filter
          ]
        );

        assignmentIdMap[item.id] = insertAssignmentRes.rows[0].id;
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
            actorId,
            actorId
          ]
        );
      }

      return {
        copied_assignments: sourceAssignmentsRes.rows.length,
        copied_column_permissions: sourceColumnPermissionsRes.rows.length
      };
    };

    try {
      await client.query("BEGIN");

      // -------------------------------------------------
      // 1) Validate target user exists
      // -------------------------------------------------
      const targetUserRes = await client.query(
        `
      SELECT id
      FROM users
      WHERE id = $1
      LIMIT 1
      `,
        [user_id]
      );

      if (!targetUserRes.rows.length) {
        throw new Error("Target user not found");
      }

      // -------------------------------------------------
      // 2) Find module source user
      // -------------------------------------------------
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
        [modules_code, user_id]
      );

      if (!moduleSourceRes.rows.length) {
        throw new Error("No user found with this module code");
      }

      const moduleOwner = moduleSourceRes.rows[0];

      // -------------------------------------------------
      // 3) Find permission source user
      // -------------------------------------------------
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
        [permission_code, user_id]
      );

      if (!permissionSourceRes.rows.length) {
        throw new Error("No user found with this permission code");
      }

      const permissionOwner = permissionSourceRes.rows[0];

      // -------------------------------------------------
      // 4) Update target user main fields
      //    module fields from module owner
      //    permission fields from permission owner
      // -------------------------------------------------
      await client.query(
        `
      UPDATE users
      SET
        modules_code = $1,
        assigned_modules = $2,
        assigned_sub_modules = $3,
        assigned_datasets = $4,
        module_permissions = $5,
        permission_code = $6,
        permissions = $7,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = $8
      `,
        [
          moduleOwner.modules_code || null,
          moduleOwner.assigned_modules || "[]",
          moduleOwner.assigned_sub_modules || "[]",
          moduleOwner.assigned_datasets || "[]",
          moduleOwner.module_permissions || null,
          permissionOwner.permission_code || null,
          permissionOwner.permissions || "[]",
          user_id
        ]
      );

      // -------------------------------------------------
      // 5) Copy permissions/assignments like bulk update
      //    final effective permission flow should come
      //    from permission code source
      // -------------------------------------------------
      await copyUserPermissions(user_id, permissionOwner.id);

      const copiedMeta = await copyAssignmentsAndColumnPermissions(
        user_id,
        permissionOwner.id,
        updated_by
      );

      await client.query("COMMIT");

      return {
        user_id,
        module_copied_from_user_id: moduleOwner.id,
        permission_copied_from_user_id: permissionOwner.id,
        modules_code: moduleOwner.modules_code || null,
        permission_code: permissionOwner.permission_code || null,
        copied_assignments: copiedMeta.copied_assignments,
        copied_column_permissions: copiedMeta.copied_column_permissions
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  },
};