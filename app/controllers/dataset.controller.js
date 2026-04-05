const {
  createHistory,
  updateHistory,
  insertData,
  getImportHistory,
  bulkUpdateDifferentRows,
  bulkDeleteDifferentRows
} = require("../models/dataset.model");
const fs = require("fs");
const { exec } = require("child_process");
const path = require("path");
const sharp = require('sharp');

const archiver = require("archiver");
const csv = require("csv-parser");
const { ADMIN_URL } = require('../config/global.js')
const { generateToken } = require("../middlewares/customFunction");
const { pool, NODE_ENV } = require('../config/config.js')
const { getSafetableName, parseLabel } = require('../utils/helper.js')

let datasetSchemaCache = null;

const datasetController = {

  importFile: async (req, res) => {
    const { block_id, block_name, district } = req.body;
    const file = req.file;

    if (!file) {
      return res.status(400).json({ error: "File missing" });
    }

    const historyId = await createHistory({
      block_id,
      block_name,
      district,
      file_name: file.originalname,
    });

    const pythonScript = path.join(__dirname, "../python/import_dataset.py");
    const csvPath = path.join(process.cwd(), "public", "uploads", path.basename(file.path));

    exec(`python "${pythonScript}" "${csvPath}" ${historyId}`, (error, stdout, stderr) => {
      if (error) console.error("Python error:", error.message);
      if (stderr) console.error("Python stderr:", stderr);
    });

    return res.json({
      success: true,
      message: "Import started in background",
      historyId,
    });
  },

  importHistory: async (req, res) => {
    try {
      const data = await getImportHistory();

      return res.json({
        success: true,
        data,
      });
    } catch (error) {
      console.error("IMPORT HISTORY ERROR:", error.message);
      return res.status(500).json({
        success: false,
        message: "Failed to fetch import history",
      });
    }
  },

  getDataSet: async (req, res) => {
    try {
      // Pagination
      const limit = Math.min(Number(req.query.limit) || 50, 100);
      const afterId = Number(req.query.afterId) || 0;

      // Detect schema ONCE
      if (!datasetSchemaCache) {
        const result = await pool.query(`
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'datasets'
          AND column_name IN ('is_active', 'isActive')
      `);

        const columns = result.rows;

        datasetSchemaCache = {
          hasIsActive: columns.some(c => c.column_name === 'is_active'),
          hasIsActiveCamel: columns.some(c => c.column_name === 'isActive')
        };
      }

      let sql;
      let params = [afterId, limit];

      if (datasetSchemaCache.hasIsActive) {
        sql = `
        SELECT
          id,
          dataset_id,
          dataset_name,
          is_active,
          created_at,
          updated_at
        FROM datasets
        WHERE is_active = true
          AND id > $1
        ORDER BY id
        LIMIT $2
      `;
      } else if (datasetSchemaCache.hasIsActiveCamel) {
        sql = `
        SELECT
          id,
          dataset_id,
          dataset_name,
          "isActive" AS is_active,
          created_at,
          updated_at
        FROM datasets
        WHERE "isActive" = true
          AND id > $1
        ORDER BY id
        LIMIT $2
      `;
      } else {
        sql = `
        SELECT
          id,
          dataset_id,
          dataset_name,
          created_at,
          updated_at
        FROM datasets
        WHERE id > $1
        ORDER BY id
        LIMIT $2
      `;
      }

      const result = await pool.query(sql, params);
      const rows = result.rows;

      const data = rows.map(row => ({
        ...row,
        is_active: row.is_active ?? true
      }));

      return res.json({
        success: true,
        data,
        nextAfterId: data.length ? data[data.length - 1].id : null
      });

    } catch (error) {
      console.error('❌ Error fetching datasets:', error);

      return res.status(500).json({
        success: false,
        message: 'Failed to fetch datasets',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  },

  getAreaMapping: async (req, res) => {
    try {
      const structureResult = await pool.query(`
      SELECT
        column_name,
        data_type,
        is_nullable,
        column_default
      FROM information_schema.columns
      WHERE table_name = 'db_table'
      ORDER BY ordinal_position
    `);

      // Get sample data
      const sampleResult = await pool.query(`
      SELECT *
      FROM db_table
      LIMIT 1
    `);

      res.json({
        success: true,
        tableStructure: structureResult.rows,
        sampleData: sampleResult.rows[0] || null,
        message: 'Table structure retrieved successfully'
      });

    } catch (error) {
      console.error('❌ Error checking table structure:', error);

      res.status(500).json({
        success: false,
        error: 'Failed to check table structure',
        details: error.message
      });
    }
  },

  fetchOptions: async (req, res) => {
    try {
      const query = `
            SELECT pc_no, pc_name, ac_no, ac_name, distt, block_city 
            FROM public.db_mappingmaster 
            ORDER BY pc_no, ac_no;
        `;
      const { rows } = await pool.query(query);

      const groupedData = rows.reduce((acc, row) => {
        let pc = acc.find(p => p.pc_no === row.pc_no);

        if (!pc) {
          pc = {
            pc_no: row.pc_no,
            pc_name: row.pc_name,
            districts: new Set(),
            blocks: new Set(),
            assemblies: []
          };
          acc.push(pc);
        }

        if (row.distt) pc.districts.add(row.distt);
        if (row.block_city) pc.blocks.add(row.block_city);

        if (!pc.assemblies.find(a => a.ac_no === row.ac_no)) {
          pc.assemblies.push({
            ac_no: row.ac_no,
            ac_name: row.ac_name
          });
        }

        return acc;
      }, []);

      const finalResponse = groupedData.map(pc => ({
        ...pc,
        districts: Array.from(pc.districts),
        blocks: Array.from(pc.blocks)
      }));

      res.json(finalResponse);

    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  },

  fetchTableData: async (req, res) => {
    try {
      let {
        ac_no = null,
        pc_no = null,
        distt = null,
        block_city = null,
        page = 1,
        limit
      } = req.query;

      const cleanAcNo = ac_no ? parseInt(ac_no) : null;
      const cleanPcNo = pc_no ? parseInt(pc_no) : null;
      const parsedPage = Math.max(1, parseInt(page) || 1);

      const parsedLimit = Math.max(1, parseInt(limit) || 50);

      const offset = (parsedPage - 1) * parsedLimit;

      if (
        (cleanAcNo === null || cleanAcNo === undefined) &&
        (cleanPcNo === null || cleanPcNo === undefined) &&
        distt === null &&
        block_city === null
      ) {
        return res.status(400).json({
          success: false,
          error:
            "At least one master filter (ac_no, pc_no, distt, or block_city) is required."
        });
      }

      const baseParams = [cleanAcNo, cleanPcNo, distt, block_city];
      const finalParams = [
        ...baseParams,
        parsedLimit,
        offset,
        ADMIN_URL
      ];

      const mainQuery = `
      WITH filtered_data AS (
          SELECT *
          FROM public.vw_db_table_with_age_surname
          WHERE
              ($1::int IS NULL OR ac_no = $1)
              AND ($2::int IS NULL OR pc_no = $2)
              AND ($3::text IS NULL OR LOWER(distt) = LOWER($3))
              AND ($4::text IS NULL OR LOWER(block_city) = LOWER($4))
      )
      SELECT json_build_object(
          'data', (
              SELECT COALESCE(json_agg(d), '[]'::json)
              FROM (
                  SELECT 
                      fd.*,
                      CASE 
                          WHEN fd.photo IS NOT NULL AND fd.photo <> ''
                          THEN $7 || fd.photo
                          ELSE NULL
                      END AS photo
                  FROM filtered_data fd
                  ORDER BY id ASC
                  LIMIT $5 OFFSET $6
              ) d
          ),
          'total', (SELECT COUNT(*) FROM filtered_data),
          'subFilters', (
              SELECT json_build_object(
                  'ru', json_agg(DISTINCT ru) FILTER (WHERE ru IS NOT NULL),
                  'gp_ward', json_agg(DISTINCT gp_ward) FILTER (WHERE gp_ward IS NOT NULL),
                  'village', json_agg(DISTINCT village) FILTER (WHERE village IS NOT NULL),
                  'cast_id', json_agg(DISTINCT cast_id) FILTER (WHERE cast_id IS NOT NULL),
                  'cast_name', json_agg(DISTINCT cast_name) FILTER (WHERE cast_name IS NOT NULL),
                  'cast_cat', json_agg(DISTINCT cast_cat) FILTER (WHERE cast_cat IS NOT NULL),
                  'sex', json_agg(DISTINCT sex) FILTER (WHERE sex IS NOT NULL),
                  'religion', json_agg(DISTINCT religion) FILTER (WHERE religion IS NOT NULL),
                  'surname', json_agg(DISTINCT surname->>'v') 
                      FILTER (WHERE surname IS NOT NULL AND surname->>'v' IS NOT NULL)
              ) FROM filtered_data
          )
      ) AS result;
    `;

      const result = await pool.query(mainQuery, finalParams);

      const response =
        result.rows[0]?.result || { data: [], total: 0, subFilters: {} };


      console.log(Math.ceil(parseInt(response.total) / parsedLimit))

      res.json({
        success: true,
        data: response.data,
        total: parseInt(response.total),
        page: parsedPage,
        limit: parsedLimit,
        totalPages: Math.ceil(parseInt(response.total) / parsedLimit),
        subFilterOptions: response.subFilters
      });
    } catch (error) {
      console.error("Error fetching table data:", error);
      res.status(500).json({
        success: false,
        error: "Server Error",
        details: error.message
      });
    }
  },

  filterFetchedTableData: async (req, res) => {

    try {

      const {

        ac_no, pc_no, distt, block_city,

        ru, gp_ward, village,

        cast_id, cast_name, cast_cat,

        mobile, surname, dob,

        vname, mname, fname,

        sex, religion,

        page = 1,

        limit = 50

      } = req.query;

      const parsedPage = Math.max(1, parseInt(page) || 1);

      const parsedLimit = Math.max(1, parseInt(limit) || 50);

      const offset = (parsedPage - 1) * parsedLimit;

      const where = [];

      const params = [];

      let i = 1;

      // 🔹 Master filters

      if (ac_no) {

        where.push(`ac_no = $${i++}`);

        params.push(parseInt(ac_no));

      }

      if (pc_no) {

        where.push(`pc_no = $${i++}`);

        params.push(parseInt(pc_no));

      }

      if (distt) {

        where.push(`LOWER(TRIM(distt)) = LOWER(TRIM($${i++}))`);

        params.push(distt);

      }

      if (block_city) {

        where.push(`LOWER(TRIM(block_city)) = LOWER(TRIM($${i++}))`);

        params.push(block_city);

      }

      // 🔹 Sub filters

      if (ru !== undefined && ru !== "") {

        where.push(`ru = $${i++}`);

        params.push(ru);

      }

      if (gp_ward) {

        where.push(`LOWER(TRIM(gp_ward)) = LOWER(TRIM($${i++}))`);

        params.push(gp_ward);

      }

      if (village) {

        where.push(`LOWER(TRIM(village)) = LOWER(TRIM($${i++}))`);

        params.push(village);

      }

      if (cast_id) {

        where.push(`cast_id = $${i++}`);

        params.push(parseInt(cast_id));

      }

      if (cast_name) {

        where.push(`LOWER(TRIM(cast_name)) = LOWER(TRIM($${i++}))`);

        params.push(cast_name);

      }

      if (cast_cat) {

        where.push(`LOWER(TRIM(cast_cat)) = LOWER(TRIM($${i++}))`);

        params.push(cast_cat);

      }

      if (mobile) {

        where.push(`(phone1 = $${i} OR phone2 = $${i})`);

        params.push(mobile);

        i++;

      }

      if (surname) {

        where.push(`surname->>'v' ILIKE $${i++}`);

        params.push(`%${surname}%`);

      }

      if (dob) {

        where.push(`dob = $${i++}`);

        params.push(dob);

      }

      if (vname) {

        where.push(`vname ILIKE $${i++}`);

        params.push(`%${vname}%`);

      }

      if (fname) {

        where.push(`fname ILIKE $${i++}`);

        params.push(`%${fname}%`);

      }

      if (mname) {

        where.push(`mname ILIKE $${i++}`);

        params.push(`%${mname}%`);

      }

      if (sex) {

        where.push(`sex = $${i++}`);

        params.push(sex);

      }

      if (religion) {

        where.push(`LOWER(TRIM(religion)) = LOWER(TRIM($${i++}))`);

        params.push(religion);

      }

      if (where.length === 0) {

        return res.status(400).json({

          success: false,

          error: "At least one filter is required"

        });

      }

      // 🔹 MAIN QUERY (same structure as fetchTableData)

      const query = `

      WITH filtered_data AS (

        SELECT *

        FROM public.vw_db_table_with_age_surname

        WHERE ${where.join(" AND ")}

      )

      SELECT json_build_object(

        'data', (

          SELECT COALESCE(json_agg(d), '[]'::json)

          FROM (

            SELECT *

            FROM filtered_data

            ORDER BY ac_no, id ASC

            LIMIT $${i++} OFFSET $${i++}

          ) d

        ),

        'total', (SELECT COUNT(*) FROM filtered_data),

        'subFilters', (

          SELECT json_build_object(

            'ru', json_agg(DISTINCT ru) FILTER (WHERE ru IS NOT NULL),

            'gp_ward', json_agg(DISTINCT gp_ward) FILTER (WHERE gp_ward IS NOT NULL),

            'village', json_agg(DISTINCT village) FILTER (WHERE village IS NOT NULL),

            'cast_id', json_agg(DISTINCT cast_id) FILTER (WHERE cast_id IS NOT NULL),

            'cast_name', json_agg(DISTINCT cast_name) FILTER (WHERE cast_name IS NOT NULL),

            'cast_cat', json_agg(DISTINCT cast_cat) FILTER (WHERE cast_cat IS NOT NULL),

            'sex', json_agg(DISTINCT sex) FILTER (WHERE sex IS NOT NULL),

            'religion', json_agg(DISTINCT religion) FILTER (WHERE religion IS NOT NULL),

            'surname', json_agg(DISTINCT surname->>'v')

              FILTER (WHERE surname IS NOT NULL AND surname->>'v' IS NOT NULL)

          )

          FROM filtered_data

        )

      ) AS result;

    `;

      params.push(parsedLimit, offset);

      const result = await pool.query(query, params);

      // console.log(result)

      const response = result.rows[0]?.result || { data: [], total: 0, subFilters: {} };

      res.json({

        success: true,

        data: response.data,

        total: parseInt(response.total),

        page: parsedPage,

        limit: parsedLimit,

        totalPages: Math.ceil(parseInt(response.total) / parsedLimit),

        subFilterOptions: response.subFilters

      });

    } catch (err) {

      console.error("Filter fetch error:", err);

      res.status(500).json({

        success: false,

        error: "Server error during filtering",

        details: err.message

      });

    }

  },

  // updateDataSet: async (req, res) => {
  //   try {
  //     const { rows } = req.body;
  //     console.log(rows)
  //     const files = req.files;

  //     console.log('fl ->>> ', files)

  //     if (!Array.isArray(rows) || rows.length === 0) {
  //       return res.status(400).json({
  //         success: false,
  //         message: 'rows must be a non-empty array'
  //       });
  //     }

  //     const photoMap = {};
  //     if (files && files.length > 0) {
  //       for (const file of files) {
  //         const id = path.basename(file.originalname, path.extname(file.originalname));
  //         photoMap[id] = `/uploads/${file.filename}`;
  //       }
  //     }

  //     const updated = await bulkUpdateDifferentRows(rows, photoMap);

  //     return res.json({
  //       success: true,
  //       updated
  //     });

  //   } catch (error) {
  //     console.error('updateDataSet error:', error);
  //     res.status(500).json({
  //       success: false,
  //       message: 'Internal server error'
  //     });
  //   }
  // },

  updateDataSet: async (req, res) => {
    try {
      let rows = req.body.rows;

      if (!Array.isArray(rows) || rows.length === 0) {
        return res.status(400).json({
          success: false,
          message: "rows must be a non-empty array"
        });
      }

      const uploadDir = path.join(process.cwd(), "public", "uploads");
      if (!fs.existsSync(uploadDir)) {
        fs.mkdirSync(uploadDir, { recursive: true });
      }

      for (const row of rows) {
        if (row?.data?.photo?.startsWith("data:image")) {
          const base64Data = row.data.photo.split(";base64,").pop();
          const buffer = Buffer.from(base64Data, "base64");

          const fileName =
            Date.now() + "-" + Math.random().toString(36).substring(7) + ".jpg";
          const filePath = path.join(uploadDir, fileName);

          await sharp(buffer)
            .jpeg({
              quality: 85,
              mozjpeg: true
            })
            .toFile(filePath);

          row.data.photo = `/uploads/${fileName}`;
        }
      }

      const updatedCount = await bulkUpdateDifferentRows(rows);

      return res.json({
        success: true,
        updatedCount
      });
    } catch (error) {
      console.error("updateDataSet error:", error);
      return res.status(500).json({
        success: false,
        message: "Internal server error"
      });
    }
  },

  deleteDataSet: async (req, res) => {
    try {
      const { ids } = req.body;

      if (!Array.isArray(ids) || ids.length === 0) {
        return res.status(400).json({
          success: false,
          message: 'ids array is required'
        });
      }

      const deletedCount = await bulkDeleteDifferentRows(ids);

      return res.json({
        success: true,
        message: `${deletedCount} record(s) deleted successfully`
      });

    } catch (error) {
      console.error('deleteDataSet error:', error);
      res.status(500).json({
        success: false,
        message: 'Internal server error'
      });
    }
  }
};

module.exports = datasetController;