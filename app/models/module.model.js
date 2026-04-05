const {pool} = require("../config/config")

async function fetchModules() {
    try {
      const { rows } = await pool.query(
        `
        SELECT
          id,
          module_id,
          module_name,
          COALESCE(is_active, true) AS is_active,
          created_at,
          updated_at
        FROM modules
        WHERE COALESCE(is_active, true) = true
        ORDER BY id ASC
        `
      );
  
      return rows;
    } catch (error) {
      console.error("fetchModules error:", error);
      throw error;
    }
  }
  
  module.exports = { fetchModules };