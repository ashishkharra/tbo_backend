const { validationResult } = require('express-validator')

function validatorMiddleware(req, res, next) {
    const errors = validationResult(req)
    if (!errors.isEmpty()) {
        return res
            .status(422)
            .json(responseData_(errors.errors[0].msg, {}, false))
    } else {
        next()
    }
}

function responseData_(message, result, success) {
    let response = {}
    response.success = success
    response.message = message
    response.results = result

    return response
}

function parseLabel(label) {
  try {
    const str = String(label || '');
    const match = str.match(/^(.*?)\s*\(([^)]+)\)$/);
    if (match) {
      return { name: match[1].trim(), code: match[2].trim() };
    }
    return { name: str.trim(), code: null };
  } catch {
    return { name: '', code: null };
  }
}

function getSafetableName(requested) {
  try {
    const raw = String(requested || '').trim();
    
    const aliasMap = {
      'village_mapping': 'village_mapping',
      'castid': 'castid',
      'master_surname': 'master_surname',
      'village_master': 'db_table',
      'ac_pc_master': 'div_dist_pc_ac',
      'dashboard': 'dashboard',
      'block_table': 'db_table',
      'tableName': 'db_table'
    };
    
    if (raw in aliasMap) return aliasMap[raw];
    
    const whitelist = [
      'db_table',
      'village_mapping',
      'castid',
      'master_surname',
      'div_dist_pc_ac',
      'dashboard',
      'election_controller',
      'voter_management',
      'geographic_data',
      'reports_analytics',
      'system_config'
    ];
    
    if (whitelist.includes(raw)) return raw;
  } catch { }
  
  return 'db_table';
}

module.exports = {
    validatorMiddleware,
    responseData_,
    parseLabel,
    getSafetableName
}