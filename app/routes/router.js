const express = require('express');
const router = express.Router();
const authRoutes = require('./auth.route');
const datasetRoute = require('./dataset.route');
const moduleRoutes = require('./module.route');
const dataAssignment = require('./data-assignment.route');
const teamRoutes = require('./team.route');
const dataIdRoutes = require('./dataId.routes.js')
const accessCheck = require('./accessCheck.route.js')

router.use('/auth', authRoutes);
router.use('/access-check', accessCheck);
router.use('/dataset', datasetRoute );
router.use('/', moduleRoutes);
router.use('/user-assignments',dataAssignment);
router.use('/team', teamRoutes);
router.use('/dataid', dataIdRoutes);

module.exports = router;