const dataIdModel = require('../models/dataId.model.js')
const { MAX_MEMBERS_PER_FAMILY } = require('../config/global.js')
const { exec } = require("child_process");
const path = require('path')
const { redisClient } = require("../config/redis.config.js");
const { generatePDF } = require('../config/pdf.service.js');
const { generatePDFHTML, generateBlankRegisterHTML } = require('../config/template.service.js');
const archiver = require('archiver')
const puppeteer = require('puppeteer');
const fs = require('fs')
const XLSX = require('xlsx')
const { requireModulePermission } = require('../middlewares/authenticateToken.js')


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


module.exports = {

    getDataIdRow: async (req, res) => {
        try {
            const { ac_no, ac_name_hi } = req.query;

            if (!ac_no && !ac_name_hi) {
                const rows = await dataIdModel.getAcList();
                const data = rows.map(r => ({
                    ac_no: r.ac_no,
                    ac_list: `${r.ac_no} - ${r.ac_name_hi}`
                }))

                return res.json({
                    success: true,
                    type: "AC_LIST",
                    data: rows.map(r => ({
                        ac_no: r.ac_no,
                        ac_list: `${r.ac_no} - ${r.ac_name_hi}`
                    }))
                });
            }

            if (!ac_no || !ac_name_hi) {
                return res.status(400).json({
                    success: false,
                    error: "Both ac_no and ac_name_hi are required"
                });
            }

            const rows = await dataIdModel.getDataIdsByAc(ac_no, ac_name_hi);

            return res.json({
                success: true,
                type: "DATA_ID_LIST",
                ac: {
                    ac_no: Number(ac_no),
                    ac_name_hi
                },
                data: rows
            });

        } catch (error) {
            console.error("getDataIdRow error:", error);
            res.status(500).json({
                success: false,
                error: "Server error"
            });
        }
    },

    getDataIdAllActiveRows: async (req, res) => {
        try {
            const page = parseInt(req.query.page) || 1;
            const limit = parseInt(req.query.limit) || 10;
            const result = await dataIdModel.getDataIdsAllActiveRows(page, limit);

            return res.json({
                success: true,
                type: "DATA_ID_LIST_ALL_ROWS",
                page: result.page,
                limit: result.limit,
                count: result.count,
                data: result.data
            });

        } catch (error) {
            console.error("getDataIdAllRow error:", error);
            res.status(500).json({
                success: false,
                error: "Server error"
            });
        }
    },

    updateDataIdRow: async (req, res) => {
        try {
            const { data_id, ac_no, ...updateFields } = req.body;

            if (!data_id || !ac_no) {
                return res.status(400).json({
                    success: false,
                    error: "data_id and ac_no are required"
                });
            }

            if (Object.keys(updateFields).length === 0) {
                return res.status(400).json({
                    success: false,
                    error: "No fields provided to update"
                });
            }

            const updated = await dataIdModel.updateDataIdByKeys(
                data_id,
                ac_no,
                updateFields
            );

            if (!updated) {
                return res.status(404).json({
                    success: false,
                    error: "Record not found"
                });
            }

            return res.json({
                success: true,
                message: "Record updated successfully"
            });

        } catch (error) {
            console.error("updateDataIdRow error:", error);
            return res.status(500).json({
                success: false,
                error: "Server error"
            });
        }
    },

    importErollData: async (req, res) => {
        const ac_no = Number(req.body.ac_no);
        const data_id = Number(req.body.data_id);
        const file = req.file;

        if (!file) {
            return res.status(400).json({ error: "File missing" });
        }

        if (!ac_no || !data_id) {
            return res.status(400).json({ error: "ac_no and data_id are required" });
        }

        const exists = await dataIdModel.checkExistdataIdAcNo(ac_no, data_id);

        if (!exists) {
            return res.status(404).json({
                success: false,
                message: "Record not found in Import Master. Please verify ac_no and data_id."
            });
        }

        const pythonScript = path.join(__dirname, "../python/import_erolldata.py");
        const csvPath = path.join(
            process.cwd(),
            "public",
            "uploads",
            path.basename(file.path)
        );

        exec(
            `python "${pythonScript}" "${csvPath}" "${ac_no}" "${data_id}"`,
            (error, stdout, stderr) => {
                if (error) console.error("Python error:", error.message);
                if (stderr) console.error("Python stderr:", stderr);
                if (stdout) console.log(stdout);
            }
        );

        return res.json({
            success: true,
            message: "Import is running in background"
        });
    },

    initGlobalCache: async () => {
        try {
            const allData = await dataIdModel.getAllForCache();

            const dataById = allData.reduce((acc, item) => {
                const slimItem = {
                    id: item.id,
                    ac_no: item.ac_no,
                    ac_name_hi: item.ac_name_hi,
                    pc_no: item.pc_no,
                    pc_name_hi: item.pc_name_hi,
                    district_id: item.district_id,
                    district_hi: item.district_hi,
                    party_district_id: item.party_district_id,
                    party_district_hi: item.party_district_hi,
                    div_id: item.div_id,
                    div_name_hi: item.div_name_hi,
                    data_range: item.data_range || [],
                    is_active: item.is_active
                };

                // If first row for this data_id, create array
                if (!acc[item.data_id]) {
                    acc[item.data_id] = [slimItem];
                } else {
                    acc[item.data_id].push(slimItem);
                }

                return acc;
            }, {});

            // Save full mapping to Redis
            await redisClient.set("master_mappings", JSON.stringify(dataById));

            // Optional: store each data_id separately for faster access
            for (const [dataId, value] of Object.entries(dataById)) {
                await redisClient.set(`data:${dataId}`, JSON.stringify(value));
            }

            return true;
        } catch (error) {
            console.error("Redis Cache Initialization Failed:", error);
            return false;
        }
    },

    getAutofill: async (req, res) => {
        try {
            const { data_id, ac_id, pc_id, district_id, div_id } = req.query;

            if (data_id && ac_id && pc_id && district_id && div_id) {
                const exactKey = `map:${data_id}:${ac_id}:${pc_id}:${district_id}:${div_id}`;
                const data = await redisClient.get(exactKey);
                if (data) {
                    return res.json({ success: true, mapping: JSON.parse(data) });
                }
            }

            const masterListRaw = await redisClient.get("master_mappings");
            if (!masterListRaw) return res.status(500).json({ error: "Cache empty" });

            const masterList = JSON.parse(masterListRaw);

            const filtered = masterList.filter(item => (
                (!data_id || item.data_id == data_id) &&
                (!ac_id || item.ac_id == Number(ac_id)) &&
                (!pc_id || item.pc_id == Number(pc_id)) &&
                (!district_id || item.district_id == Number(district_id)) &&
                (!div_id || item.div_id == Number(div_id))
            ));

            res.json({
                success: true,
                count: filtered.length,
                mapping: filtered.length === 1 ? filtered[0] : null,
                data: filtered
            });
        } catch (err) {
            console.log('autofill error : ', err)
            res.status(500).json({ success: false, error: err.message });
        }
    },

    getAllMappings: async (req, res) => {
        try {
            const data = await redisClient.get("master_mappings");
            if (!data) return res.status(500).json({ error: "Cache empty" });
            res.json({ success: true, data: JSON.parse(data) });
        } catch (err) {
            res.status(500).json({ success: false, error: err.message });
        }
    },

    getVotersList: async (req, res) => {
        try {
            const filters = {
                data_id: req.query.data_id,
                ac_id: req.query.ac_id,
                pc_id: req.query.pc_id,
                district_id: req.query.district_id,
            };

            const limit = parseInt(req.query.limit) || 100;
            const page = parseInt(req.query.page) || 1;
            const offset = (page - 1) * limit;

            const result = await dataIdModel.filterVoters(req.user, filters, limit, offset);

            res.status(200).json({
                success: true,
                mapping: result.mapping,
                metadata: {
                    totalRecords: result.total,
                    currentPage: page,
                    totalPages: Math.ceil(result.total / limit)
                },
                voters: result.voters
            });

        } catch (error) {
            console.error(error);
            res.status(500).json({ success: false, error: error.message });
        }
    },

    getAdvancedVotersList: async (req, res) => {
        try {
            const filters = Object.fromEntries(
                Object.entries(req.query).map(([key, value]) => [
                    key,
                    value === "All" ? "" : value,
                ])
            );

            const limit = parseInt(req.query.limit) || 100;
            const page = parseInt(req.query.page) || 1;
            const offset = (page - 1) * limit;

            const result = await dataIdModel.applyAdvancedFilter(
                req.user,
                filters,
                limit,
                offset
            );

            res.json({
                success: true,
                total: result.total,
                currentPage: page,
                totalPages: Math.ceil(result.total / limit),
                voters: result.voters,
                mapping: result.mapping,
            });
        } catch (error) {
            console.error("Controller Error:", error);
            res.status(500).json({
                success: false,
                error: error.message,
            });
        }
    },

    bulkUpdateDataIdVoters: async (req, res) => {
        try {
            let updates = req.body;

            if (!Array.isArray(updates)) {
                updates = [updates];
            }

            if (req.files && req.files.length > 0) {

                req.files.forEach((file, index) => {
                    if (updates[index]) {
                        updates[index].photo = file.filename;
                    }
                });
            }

            await dataIdModel.bulkUpdateVoters(updates);

            res.json({
                success: true,
                message: "Voter(s) updated successfully"
            });

        } catch (error) {
            console.error("Bulk Update Error:", error);

            res.status(500).json({
                success: false,
                message: error.message || "Internal Server Error"
            });
        }
    },

    deleteMasterRow: async (req, res) => {
        try {
            const result = await dataIdModel.deleteMasterRow(req.body);

            return res.status(result.success ? 200 : 400).json(result);
        } catch (error) {
            console.error("deleteMasterRow controller error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal Server Error",
                error: error.message
            });
        }
    },

    saveMasterPatch: async (req, res) => {
        try {
            const { table } = req.body
            let module = null, action = 'update';
            if (table === 'dataid_importmaster') {
                module = 'voter_list_data_id_master';
            } else if (table === 'eroll_castmaster') {
                module = 'voter_list_cast_id_master';
            } else if (table === 'eroll_yojna_master') {
                module = 'voter_list_yojna_master'
            } else {
                module = 'voter_list_other_master'
            }
            requireModulePermission(module, action)
            const result = await dataIdModel.saveMasterPatch(req.body);

            return res.status(result.success ? 200 : 400).json(result);
        } catch (error) {
            console.error("saveMasterPatch controller error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal Server Error",
                error: error.message
            });
        }
    },

    // register print
    getWiseCast: async (req, res) => {
        try {

            const { data_id, wise_type } = req.query;

            if (!data_id || !wise_type) {
                return res.status(400).json({
                    success: false,
                    message: "data_id and wise_type are required"
                });
            }

            const result = await dataIdModel.getDataByDataIdWiseType(
                Number(data_id),
                wise_type
            );

            return res.status(200).json({
                success: true,
                data: {
                    [wise_type]: result.wise || [],
                    castids: result.castids || []
                }
            });

        } catch (error) {
            console.error("Wise cast error:", error);

            return res.status(500).json({
                success: false,
                message: error.message || "Internal server error"
            });
        }
    },

    printRegister: async (req, res) => {
        try {
            const body = req.body;

            const filters = {
                ...body,
                bhag: body?.filters?.bhag
            };

            // ===== FETCH DATA =====
            const { erollData = [] } =
                await dataIdModel.getFilteredData(filters);

            if (!erollData.length) {
                return res.status(404).json({
                    success: false,
                    message: "No data found"
                });
            }

            let filteredData = [...erollData];

            // ===== STEP 1: REMOVE DUPLICATES =====
            const seenVoters = new Set();
            const uniqueData = [];

            filteredData.forEach(member => {
                const voterKey = `${member.vsno}_${member.familyid}_${member.section}`;

                if (!seenVoters.has(voterKey)) {
                    seenVoters.add(voterKey);
                    uniqueData.push(member);
                }
            });

            filteredData = uniqueData;

            const excludes = filters.excludes || {};

            // Handle excludeCasts array separately
            const excludeCasts = excludes.excludeCasts || [];

            if (Object.keys(excludes).length > 0 || excludeCasts.length > 0) {
                // Track initial count
                const initialCount = filteredData.length;

                // Apply exclude filters
                filteredData = filteredData.filter(voter => {
                    let shouldInclude = true;

                    if (excludeCasts.length > 0) {
                        // Check if voter's castid is in the exclude list
                        if (voter.castid && excludeCasts.includes(voter.castid)) {
                            shouldInclude = false;
                            return false; // Immediately exclude
                        }

                        // Also check cast_cat if needed
                        if (voter.cast_cat && excludeCasts.includes(voter.cast_cat)) {
                            shouldInclude = false;
                            return false; // Immediately exclude
                        }
                    }

                    // Check each exclude condition (only if not already excluded)
                    for (const [key, shouldExclude] of Object.entries(excludes)) {
                        // Skip excludeCasts as it's already handled
                        if (key === 'excludeCasts') continue;

                        if (!shouldExclude) continue; // Skip if exclude is false

                        switch (key) {
                            case 'dob':
                                // Exclude if voter has DOB (not null, not empty)
                                if (voter.dob && voter.dob !== null && voter.dob !== '') {
                                    shouldInclude = false;
                                }
                                break;

                            case 'mobile':
                                // Exclude if voter has mobile/phone (phone1 field)
                                if (voter.phone1 && voter.phone1 !== null && voter.phone1 !== '') {
                                    shouldInclude = false;

                                }
                                break;

                            case 'cast':
                            case 'caste':
                                // Exclude if voter has caste/cast_cat (but not if already handled by excludeCasts)
                                if (voter.cast_cat && voter.cast_cat !== null && voter.cast_cat !== '') {
                                    // Check if this caste is not in excludeCasts (to avoid double logging)
                                    if (!excludeCasts.includes(voter.cast_cat)) {
                                        shouldInclude = false;

                                    }
                                }
                                break;

                            case 'age':
                                // Exclude if voter has age
                                if (voter.age && voter.age !== null && voter.age > 0) {
                                    shouldInclude = false;

                                }
                                break;

                            case 'epic':
                                // Exclude if voter has epic
                                if (voter.epic && voter.epic !== null && voter.epic !== '') {
                                    shouldInclude = false;

                                }
                                break;

                            case 'email':
                                // Exclude if voter has email
                                if (voter.email && voter.email !== null && voter.email !== '') {
                                    shouldInclude = false;
                                }
                                break;

                            default:
                                // Generic field exclude - check if field exists and has value
                                if (voter[key] && voter[key] !== null && voter[key] !== '') {
                                    shouldInclude = false;

                                }
                        }

                        // Break early if we already decided to exclude
                        if (!shouldInclude) break;
                    }

                    return shouldInclude;
                });

                // Log caste exclusion summary
                if (excludeCasts.length > 0) {
                }
            }
            const includes = filters.includes || {};

            filteredData = filteredData.map(row => {
                const newRow = { ...row };

                if (includes.dob === false) delete newRow.dob;
                if (includes.mobile === false) delete newRow.phone1;
                if (includes.cast_cat === false) delete newRow.cast_cat;
                if (includes.age === false) delete newRow.age;
                if (includes.epic === false) delete newRow.epic;

                return newRow;
            });

            // ===== STEP 4: APPLY FAMILY COUNT FILTER =====
            if (Number(filters.familyCount) > 0) {
                const minCount = Number(filters.familyCount);

                // Group by familyid AND section
                const familySectionGroups = {};

                filteredData.forEach(row => {
                    const section = row.section || 'nosection';
                    const key = `${row.familyid}_${section}`;

                    if (!familySectionGroups[key]) {
                        familySectionGroups[key] = [];
                    }
                    familySectionGroups[key].push(row);
                });

                // Keep ONLY groups with minCount members
                const validGroups = Object.values(familySectionGroups)
                    .filter(group => group.length >= minCount);

                const limitedGroups = validGroups.map(group => {
                    if (group.length > MAX_MEMBERS_PER_FAMILY) {
                        return group.slice(0, MAX_MEMBERS_PER_FAMILY);
                    }
                    return group;
                });

                // Flatten back to rows
                filteredData = limitedGroups.flat();

            }

            // ===== STEP 5: SORT DATA =====
            filteredData.sort((a, b) => {
                // First by bhag_no
                const bhagA = parseInt(a.bhag_no) || 0;
                const bhagB = parseInt(b.bhag_no) || 0;
                if (bhagA !== bhagB) return bhagA - bhagB;

                // Then by section
                const sectionA = a.section || '';
                const sectionB = b.section || '';
                if (sectionA !== sectionB) return sectionA.localeCompare(sectionB);

                // Then by vsno
                const vsnoA = parseInt(a.vsno) || 0;
                const vsnoB = parseInt(b.vsno) || 0;
                return vsnoA - vsnoB;
            });

            // ===== STEP 6: GROUP BY BHAG =====
            const bhagGroups = {};

            filteredData.forEach(row => {
                if (!bhagGroups[row.bhag_no]) {
                    bhagGroups[row.bhag_no] = [];
                }
                bhagGroups[row.bhag_no].push(row);
            });

            // ===== STEP 7: GENERATE OUTPUT (PDF or ZIP) =====
            if (filters.singlePdf === true) {
                let combinedHTML = "";

                Object.keys(bhagGroups).forEach(bhag => {
                    combinedHTML += generatePDFHTML(bhagGroups[bhag]);
                });

                const pdfData = await generatePDF(combinedHTML);

                const pdfBuffer = Buffer.isBuffer(pdfData)
                    ? pdfData
                    : Buffer.from(pdfData, "base64");

                res.setHeader("Content-Type", "application/pdf");
                res.setHeader(
                    "Content-Disposition",
                    "attachment; filename=all_bhag_register.pdf"
                );

                return res.send(pdfBuffer);
            }

            res.setHeader("Content-Type", "application/zip");
            res.setHeader(
                "Content-Disposition",
                "attachment; filename=registers.zip"
            );

            const archive = archiver("zip", {
                zlib: { level: 9 }
            });

            archive.on("error", err => {
                console.error("Archive Error:", err);
                if (!res.headersSent) {
                    res.status(500).json({
                        success: false,
                        message: "Zip creation failed"
                    });
                }
            });

            archive.pipe(res);

            for (let bhag in bhagGroups) {
                const html = generatePDFHTML(bhagGroups[bhag]);

                const pdfData = await generatePDF(html);

                let pdfBuffer;

                if (Buffer.isBuffer(pdfData)) {
                    pdfBuffer = pdfData;
                } else if (typeof pdfData === "string") {
                    pdfBuffer = Buffer.from(pdfData, "base64");
                } else if (pdfData instanceof Uint8Array) {
                    pdfBuffer = Buffer.from(pdfData);
                } else {
                    throw new Error(`Invalid PDF type for bhag ${bhag}`);
                }

                archive.append(pdfBuffer, {
                    name: `bhag_${bhag}.pdf`
                });
            }

            await archive.finalize();

        } catch (error) {
            console.error("Print Register Error:", error);

            if (!res.headersSent) {
                return res.status(500).json({
                    success: false,
                    message: "Internal Server Error"
                });
            }
        }
    },

    downloadBlankRegister: async (req, res) => {
        try {
            const browser = await puppeteer.launch({
                headless: "new",
            });

            const page = await browser.newPage();

            const html = generateBlankRegisterHTML();

            await page.setContent(html, {
                waitUntil: "domcontentloaded",
                timeout: 0
            });

            const pdfData = await page.pdf({
                format: "A4",
                printBackground: true,
            });

            await browser.close();

            const pdfBuffer = Buffer.from(pdfData);

            res.set({
                "Content-Type": "application/pdf",
                "Content-Disposition": "attachment; filename=blank-register.pdf",
            });

            res.send(pdfBuffer);

        } catch (error) {
            console.error("Blank Register Error:", error);
            res.status(500).json({
                success: false,
                message: "Failed to generate blank register",
            });
        }
    },

    // master tables data id importmaster
    getDataidImportmasterTable: async (req, res) => {
        try {
            const data = await dataIdModel.getDynamicTable(req.query);
            res.json({
                success: true,
                data: data
            });
        } catch (error) {
            console.error("Get dataid table Error:", error);
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    addInDataIdImportMaster: async (req, res) => {
        try {
            const data = req.body;
            const { table } = data;

            if (!table || !insertTableConfig[table]) {
                return res.status(400).json({
                    success: false,
                    message: "Invalid table name"
                });
            }

            if (!data.data_id) {
                return res.status(400).json({
                    success: false,
                    message: "data_id is required"
                });
            }

            const existing = await dataIdModel.getExistingMasterRecord(table, data);

            if (existing) {
                return res.status(409).json({
                    success: false,
                    message: `Record already exists in ${table}`
                });
            }

            const result = await dataIdModel.insertMaster(data);

            return res.status(201).json({
                success: true,
                message: "Master record created successfully",
                data: result
            });
        } catch (error) {
            console.error("Create Master Error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal Server Error",
                error: error.message
            });
        }
    },

    addMasterTableRow: async (req, res) => {
        try {
            let module = null, action = 'create';
            if (table === 'dataid_importmaster') {
                module = 'voter_list_data_id_master';
            } else if (table === 'eroll_castmaster') {
                module = 'voter_list_cast_id_master';
            } else if (table === 'eroll_yojna_master') {
                module = 'voter_list_yojna_master'
            } else {
                module = 'voter_list_other_master'
            }
            requireModulePermission(module, action)
            const data = await dataIdModel.addDynamicMasterRow(req.body);

            res.json({
                success: true,
                message: 'Row added successfully',
                data
            });
        } catch (error) {
            console.error('Add master row Error:', error);
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    deleteRecords: async (req, res) => {
        try {
            const { table } = req.query;
            const { ids } = req.body;

            if (!table) {
                return res.status(400).json({
                    success: false,
                    message: "Table name is required"
                });
            }

            if (!Array.isArray(ids) || ids.length === 0) {
                return res.status(400).json({
                    success: false,
                    message: "ids must be a non-empty array"
                });
            }

            let module = null, action = 'delete';
            if (table === 'dataid_importmaster') {
                module = 'voter_list_data_id_master';
            } else if (table === 'eroll_castmaster') {
                module = 'voter_list_cast_id_master';
            } else if (table === 'eroll_yojna_master') {
                module = 'voter_list_yojna_master'
            } else {
                module = 'voter_list_other_master'
            }
            requireModulePermission(module, action)

            const result = await dataIdModel.deleteDynamic(table, ids);

            res.json({
                success: true,
                message: "Records deleted successfully",
                deleted: result
            });

        } catch (error) {
            console.error("Delete Error:", error);
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    syncByDataId: async (req, res) => {
        try {
            const updates = req.body;
            const result = await dataIdModel.syncByDataIdBatch(updates);
            res.json({ success: true, updated: result });
        } catch (error) {
            console.error("Error syncing data_id:", error);
            res.status(500).json({ success: false, message: error.message });
        }
    },

    downloadMasterExcel: async (req, res) => {
        try {
            const filters = {
                table: req.query.table,
                data_id: req.query.data_id,
                is_active: req.query.is_active,
                dropdown_name: req.query.dropdown_name,
                reg_name: req.query.reg_name,
                value_id: req.query.value_id,
                search: req.query.search
            };

            let module = null, action = 'export';
            if (table === 'dataid_importmaster') {
                module = 'voter_list_data_id_master';
            } else if (table === 'eroll_castmaster') {
                module = 'voter_list_cast_id_master';
            } else if (table === 'eroll_yojna_master') {
                module = 'voter_list_yojna_master'
            } else {
                module = 'voter_list_other_master'
            }
            requireModulePermission(module, action)

            const result = await dataIdModel.getMasterForExcel(filters);

            if (!result || !result.rows || result.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    message: "No data found",
                    data: []
                });
            }

            const XLSX = require("xlsx");

            const worksheet = XLSX.utils.json_to_sheet(result.rows);
            const workbook = XLSX.utils.book_new();

            XLSX.utils.book_append_sheet(workbook, worksheet, result.sheetName);

            const fileBuffer = XLSX.write(workbook, {
                type: "buffer",
                bookType: "xlsx"
            });

            const fileName = `${filters.table}_${Date.now()}.xlsx`;

            res.setHeader(
                "Content-Type",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            );
            res.setHeader(
                "Content-Disposition",
                `attachment; filename="${fileName}"`
            );

            return res.send(fileBuffer);
        } catch (error) {
            console.log("downloadMasterExcel error => ", error);
            return res.status(500).json({
                success: false,
                message: error.message || "Failed to download excel",
                data: {}
            });
        }
    },

    downloadErollMappingExcel: async (req, res) => {
        try {
            const filters = {
                data_id: req.query.data_id,
                is_active: req.query.is_active,
                ac_id: req.query.ac_id,
                bhag_no: req.query.bhag_no,
                sec_no: req.query.sec_no,
                village: req.query.village,
                gp_ward: req.query.gp_ward,
                block: req.query.block,
                psb: req.query.psb,
                coordinate: req.query.coordinate,
                kendra: req.query.kendra,
                mandal: req.query.mandal,
                pjila: req.query.pjila,
                postoff: req.query.postoff,
                policst: req.query.policst,
                search: req.query.search
            };

            const rows = await dataIdModel.getErollMappingForExcel(filters);

            if (!rows || rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    message: "No data found",
                    data: []
                });
            }

            const formattedRows = rows.map((item, index) => ({
                S_NO: index + 1,
                ID: item.id,
                DATA_ID: item.data_id,
                AC_ID: item.ac_id,
                AC_NAME: item.ac_name,
                BHAG_NO: item.bhag_no,
                BHAG: item.bhag,
                SEC_NO: item.sec_no,
                SECTION: item.section,
                RU: item.ru,
                VILLAGE: item.village,
                GP_WARD: item.gp_ward,
                BLOCK: item.block,
                PSB: item.psb,
                COORDINATE: item.coordinate,
                KENDRA: item.kendra,
                MANDAL: item.mandal,
                PJILA: item.pjila,
                PINCODE: item.pincode,
                POSTOFF: item.postoff,
                POLICST: item.policst,
                IS_ACTIVE: item.is_active,
                UPDATED_AT: item.updated_at,
                VILLAGE_ID: item.village_id,
                GP_WARD_ID: item.gp_ward_id,
                BLOCK_ID: item.block_id,
                PSB_ID: item.psb_id,
                COORDINATE_ID: item.coordinate_id,
                KENDRA_ID: item.kendra_id,
                MANDAL_ID: item.mandal_id,
                PJILA_ID: item.pjila_id,
                PINCODE_ID: item.pincode_id,
                POSTOFF_ID: item.postoff_id,
                POLICST_ID: item.policst_id,
                UPDATE_BY: item.update_by
            }));

            const worksheet = XLSX.utils.json_to_sheet(formattedRows);
            const workbook = XLSX.utils.book_new();

            XLSX.utils.book_append_sheet(workbook, worksheet, "ErollMapping");

            const fileBuffer = XLSX.write(workbook, {
                type: "buffer",
                bookType: "xlsx"
            });

            const fileName = `eroll_mapping_${Date.now()}.xlsx`;

            res.setHeader(
                "Content-Type",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            );
            res.setHeader(
                "Content-Disposition",
                `attachment; filename="${fileName}"`
            );

            return res.send(fileBuffer);
        } catch (error) {
            console.log("downloadErollMappingExcel error => ", error);
            return res.status(500).json({
                success: false,
                message: error.message || "Failed to download eroll mapping excel",
                data: {}
            });
        }
    },

    // mapping tables eroll mapping
    getErollMappingTable: async (req, res) => {
        try {
            const data = await dataIdModel.getErollMappingDynamic(req.query);
            res.json({
                success: true,
                data
            });
        } catch (error) {
            console.error("Get eroll_mapping Error:", error);
            res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    addEmptyImportMasterRow: async (req, res) => {
        try {
            const result = await dataIdModel.insertEmptyImportMasterRow();

            return res.status(200).json({
                success: true,
                message: "Empty row created successfully",
                data: result,
            });
        } catch (error) {
            console.log("addEmptyImportMasterRow error => ", error);

            return res.status(500).json({
                success: false,
                message: error.message || "Failed to create empty row",
                data: {},
            });
        }
    },

    uploadMappingOverride: async (req, res) => {
        try {
            if (!req.file) {
                return res.status(400).json({
                    success: false,
                    message: "No file uploaded"
                });
            }

            const { data_id } = req.body;

            if (!data_id) {
                // Delete uploaded file
                fs.unlinkSync(req.file.path);
                return res.status(400).json({
                    success: false,
                    message: "data_id is required"
                });
            }

            try {
                const result = await dataIdModel.processMappingFile(
                    req.file.path,
                    parseInt(data_id)
                );

                // Delete uploaded file after processing
                fs.unlinkSync(req.file.path);

                return res.status(200).json({
                    success: true,
                    message: "Mapping override completed successfully",
                    stats: result
                });

            } catch (processError) {
                // Delete uploaded file if processing fails
                if (fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }

                return res.status(500).json({
                    success: false,
                    message: processError.message
                });
            }
        } catch (error) {
            console.error("Upload Error:", error);
            return res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    downloadMappingExcel: async (req, res) => {
        try {
            const { data_id } = req.query;

            if (!data_id) {
                return res.status(400).json({
                    success: false,
                    message: "data_id is required"
                });
            }

            const filePath = await dataIdModel.generateExcel(data_id);

            return res.download(filePath, `mapping_${data_id}.xlsx`);

        } catch (error) {
            console.error("Download Error:", error);
            return res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    // update mappings
    updateMapping: async (req, res) => {
        try {
            const updates = req.body;
            const result = await dataIdModel.updateMappingBatch(updates);
            res.json({ success: true, updated: result });
        } catch (error) {
            console.error("Error updating eroll mapping:", error);
            res.status(500).json({ success: false, message: error.message });
        }
    },

    syncMappingToEroll: async (req, res) => {
        try {
            const { data_id } = req.body;

            if (!data_id) {
                return res.status(400).json({
                    success: false,
                    message: "data_id is required"
                });
            }

            const result = await dataIdModel.syncMappingToErollDb(data_id);

            return res.status(200).json({
                success: true,
                message: "Sync completed successfully",
                stats: result
            });

        } catch (error) {
            console.error("Sync Mapping Error:", error);
            return res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    updateMappingFromDb: async (req, res) => {
        try {
            const updates = req.body;
            const result = await dataIdModel.updateMappingFromDbBatch(updates);
            res.json({ success: true, updated: result });
        } catch (error) {
            console.error("Error updating mapping from DB:", error);
            res.status(500).json({ success: false, message: error.message });
        }
    },

    generateSurname: async (req, res) => {
        try {
            const { data_id } = req.body;

            if (!Array.isArray(data_id) || data_id.length === 0) {
                return res.status(400).json({ success: false, message: "No data IDs provided" });
            }

            await dataIdModel.generateSurname(data_id);

            return res.status(200).json({
                success: true,
                message: "Surnames are generated successfully",
            });
        } catch (error) {
            console.log("Error generating surnames:", error);
            return res.status(500).json({
                success: false,
                message: error.message,
            });
        }
    },

    generateFamilyIds: async (req, res) => {
        try {
            const { data_id } = req.body;

            if (!Array.isArray(data_id) || data_id.length === 0) {
                return res.status(400).json({ success: false, message: "No data IDs provided" });
            }

            await dataIdModel.generateFamilyIds(data_id);

            return res.status(200).json({
                success: true,
                message: "Family ids are generated successfully",
            });
        } catch (error) {
            console.log("Error generating family ids:", error);
            return res.status(500).json({
                success: false,
                message: error.message,
            });
        }
    },

    getRegister: async (req, res) => {
        try {
            const { data_id } = req.query;
            const register = await dataIdModel.getRegister(data_id)
            return res.status(200).json({
                success: true,
                data: register
            })
        } catch (error) {
            console.log('Get register error : ', error)
            return res.status(500).json({
                success: false,
                message: error.message
            })
        }
    },

    generateMappingids: async (req, res) => {
        try {
            const { data_id } = req.body
            await dataIdModel.generateMappingids(data_id)
            return res.status(200).json({
                success: true,
                message: 'Ids generated successfully'
            })
        } catch (error) {
            console.log('Generating ids error : ', error)
            return res.status(500).json({
                success: false,
                message: error.message
            })
        }
    },

    getYojnaList: async (req, res) => {
        try {
            const { data_id } = req.body;
            if (!data_id) {
                return res.status(400).json({
                    success: false,
                    message: "data_id is required"
                });
            }
            const yojnaList = await dataIdModel.getYojnaList(data_id);
            return res.status(200).json({
                success: true,
                data: yojnaList
            });
        } catch (error) {
            console.log("Error fetching yojna list:", error);
            return res.status(500).json({
                success: false,
                message: error.message
            });
        }
    },

    syncSurname: async (req, res) => {
        try {
            console.log('yyyyyyyyy')
            const result = await dataIdModel.syncSurname(req);

            console.log('tttttttt', result)

            return res.status(200).json({
                success: true,
                message: "Surnames synced successfully",
                result,
            });
        } catch (error) {
            console.error("syncSurname controller error:", error);

            return res.status(500).json({
                success: false,
                message: error.message || "Server error",
            });
        }
    },

    addEmptyRows: async (req, res) => {
        try {
            let module = null, action = 'add_row';
            if (table === 'dataid_importmaster') {
                module = 'voter_list_data_id_master';
            } else if (table === 'eroll_castmaster') {
                module = 'voter_list_cast_id_master';
            } else if (table === 'eroll_yojna_master') {
                module = 'voter_list_yojna_master'
            } else {
                module = 'voter_list_other_master'
            }
            requireModulePermission(module, action)
            const result = await dataIdModel.addEmptyRows(req);
            return res.status(result.success ? 201 : 400).json(result);
        } catch (error) {
            console.error(`adding row error in table ${req.body.table} of data id ${req.body.data_id}`, error);
            return res.status(500).json({
                success: false,
                message: 'Server error'
            });
        }
    },

    importMasterCsv: async (req, res) => {
        try {
            const { table } = req.body;
            const file = req.file;

            if (!table) {
                return res.status(400).json({
                    success: false,
                    message: "table is required"
                });
            }

            if (!file) {
                return res.status(400).json({
                    success: false,
                    message: "CSV file is required"
                });
            }

            let module = null, action = 'import';

            if (table === 'eroll_castmaster') {
                module = 'voter_list_cast_id_master';
            } else if (table === 'eroll_yojna_master') {
                module = 'voter_list_yojna_master'
            } else if (table === 'eroll_dropdown') {
                module = 'voter_list_other_master'
            }

            requireModulePermission(module, action)

            const result = await dataIdModel.importMasterCsv({
                table,
                fileBuffer: file.buffer,
                originalName: file.originalname
            });

            return res.status(result.success ? 200 : 400).json(result);
        } catch (error) {
            console.error("importMasterCsv controller error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal Server Error",
                error: error.message
            });
        }
    },
}