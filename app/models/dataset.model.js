const { pool } = require("../config/config");

// const ALLOWED_COLUMNS = new Set([
//   'token',
//   'family_id',
//   'vname',
//   'mname',
//   'fname',
//   'dob',
//   'sex',
//   'phone1',
//   'phone2',
//   'distt',
//   'block_city',
//   'gp_ward',
//   'village',
//   'address',
//   'cast_cat',
//   'cast_name',
//   'cast_id',
//   'religion',
//   'ru',
//   'pc_no',
//   "photo"
// ]);

async function createHistory(importData) {
  try {
    const query = `
      INSERT INTO import_history
      (block_id, block_name, district, file_name, status)
      VALUES ($1,$2,$3,$4,'processing')
      RETURNING id
    `;

    const values = [
      importData.block_id,
      importData.block_name,
      importData.district,
      importData.file_name
    ];

    const result = await pool.query(query, values);
    return result.rows[0].id;

  } catch (error) {
    throw error;
  }
}

async function updateHistory(
  historyId,
  totalRecords,
  importedRecords,
  failedRecords,
  status
) {
  try {
    const query = `
      UPDATE import_history
      SET total_records=$1,
          imported_records=$2,
          failed_records=$3,
          status=$4
      WHERE id=$5
    `;

    const values = [
      totalRecords,
      importedRecords,
      failedRecords,
      status,
      historyId
    ];

    await pool.query(query, values);

  } catch (error) {
    throw error;
  }
}

async function insertData(rowToken, rowData = {}) {
  try {
    const query = `
        INSERT INTO db_table
        (
          token, family_id, vname, fname, mname, dob, sex,
          phone1, phone2, distt, block_city, gp_ward,
          ru, ac_no, pc_no, update_by
        )
        VALUES
        (
          $1,$2,$3,$4,$5,$6,
          $7,$8,$9,$10,$11,
          $12,$13,$14,$15,$16
        )
        ON CONFLICT (family_id) DO NOTHING
      `;

    const values = [
      rowToken,
      rowData.family_id ?? null,
      rowData.vname ?? null,
      rowData.fname ?? null,
      rowData.mname ?? null,
      rowData.dob || "2000-01-01",
      rowData.sex ?? null,
      rowData.phone1 ?? null,
      rowData.phone2 ?? null,
      rowData.distt ?? null,
      rowData.block_city ?? null,
      rowData.gp_ward ?? null,
      Number(rowData.ru) || 0,
      Number(rowData.ac_no) || 0,
      Number(rowData.pc_no) || 0,
      new Date().toISOString().split("T")[0]
    ];

    const result = await pool.query(query, values);

    // rowCount = 0 → duplicate skipped
    if (result.rowCount === 0) {
      return false;
    }

    return true;

  } catch (error) {
    console.error("DB INSERT ERROR:", error.message);
    throw error;
  }
}

async function getImportHistory() {
  const query = `
    SELECT
      id,
      block_id,
      block_name,
      district,
      file_name,
      total_records,
      imported_records,
      failed_records,
      status,
      created_at
    FROM import_history
    ORDER BY created_at DESC
  `;
  try {
    const { rows } = await pool.query(query);
    return rows;
  } catch (error) {
    throw error;

  }

};

// async function bulkUpdateDifferentRows(rows, photoMap = {}) {
//   const client = await pool.connect();

//   try {
//     await client.query("BEGIN");

//     console.log(photoMap)

//     let updatedCount = 0;

//     const columnRes = await client.query(`
//       SELECT column_name
//       FROM information_schema.columns
//       WHERE table_schema = 'public'
//       AND table_name = 'db_table'
//     `);

//     const allColumns = columnRes.rows.map(r => r.column_name);

//     const restrictedColumns = new Set([
//       "id",
//       "ac_no",
//       "created_at"
//     ]);

//     const updatableColumns = new Set(
//       allColumns.filter(col => !restrictedColumns.has(col))
//     );

//     for (const row of rows) {
//       const { id, data } = row;
//       if (!id || !data || typeof data !== "object") continue;

//       if (photoMap[id]) {
//         data.photo = photoMap[id];
//       }

//       const findRes = await client.query(
//         `SELECT ac_no FROM public.db_table WHERE id = $1 LIMIT 1`,
//         [id]
//       );

//       if (findRes.rowCount === 0) continue;

//       const existingAcNo = findRes.rows[0].ac_no;

//       const keys = Object.keys(data).filter(k => updatableColumns.has(k));
//       if (keys.length === 0) continue;

//       const values = [];
//       const setClauses = keys.map((key, idx) => {
//         values.push(data[key]);
//         return `"${key}" = $${idx + 1}`;
//       });

//       values.push(existingAcNo);
//       values.push(id);

//       const updateSql = `
//         UPDATE public.db_table
//         SET ${setClauses.join(", ")}
//         WHERE ac_no = $${values.length - 1}
//           AND id = $${values.length}
//       `;

//       const updateRes = await client.query(updateSql, values);
//       updatedCount += updateRes.rowCount;
//     }

//     await client.query("COMMIT");
//     return updatedCount;

//   } catch (err) {
//     await client.query("ROLLBACK");
//     throw err;
//   } finally {
//     client.release();
//   }
// }

async function bulkUpdateDifferentRows(rows) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    let updatedCount = 0;

    const columnRes = await client.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
      AND table_name = 'db_table'
    `);

    const allColumns = columnRes.rows.map(r => r.column_name);

    const restrictedColumns = new Set([
      "id",
      "ac_no",
      "created_at"
    ]);

    const updatableColumns = new Set(
      allColumns.filter(col => !restrictedColumns.has(col))
    );

    for (const row of rows) {
      const { id, data } = row;

      if (!id || !data || typeof data !== "object") continue;

      const keys = Object.keys(data).filter(k =>
        updatableColumns.has(k)
      );

      if (keys.length === 0) continue;

      const values = [];
      const setClauses = keys.map((key, idx) => {
        values.push(data[key]);
        return `"${key}" = $${idx + 1}`;
      });

      values.push(id);

      const updateSql = `
        UPDATE public.db_table
        SET ${setClauses.join(", ")}
        WHERE id = $${values.length}
      `;

      const updateRes = await client.query(updateSql, values);
      updatedCount += updateRes.rowCount;
    }

    await client.query("COMMIT");
    return updatedCount;

  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function bulkDeleteDifferentRows(ids) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const findRes = await client.query(
      `SELECT id, ac_no FROM public.db_table WHERE id = ANY($1)`,
      [ids]
    );

    if (findRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return 0;
    }

    const rowsToDelete = findRes.rows.map(r => r.id);

    const deleteRes = await client.query(
      `DELETE FROM public.db_table WHERE id = ANY($1)`,
      [rowsToDelete]
    );

    await client.query('COMMIT');
    return deleteRes.rowCount;

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  createHistory,
  updateHistory,
  insertData,
  getImportHistory,
  bulkUpdateDifferentRows,
  bulkDeleteDifferentRows
};