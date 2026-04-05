const express = require('express');
const router = express.Router();
const  modulesController = require("../controllers/module.controller");

router.get('/modules', modulesController.getModules);
module.exports = router;