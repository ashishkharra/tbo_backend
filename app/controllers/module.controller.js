const { fetchModules } = require("../models/module.model");

const moduleController = {
    getModules:async(req, res)=> {
        try {
          const modules = await fetchModules();
          return res.json({
            success: true,
            data: modules,
          });
        } catch (error) {
          console.error("getModules error:", error);
      
          return res.status(500).json({
            success: false,
            message: "Failed to fetch modules",
          });
        }
    }
}

module.exports = moduleController;