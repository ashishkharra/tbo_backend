const { v4: uuidv4 } = require('uuid');

const idGenerator = {
    // Generate block ID
    generateBlockId: (blockName) => {
        const uuid = uuidv4().replace(/-/g, '');
        const blockRef = uuid.substring(0, 6);     // 6 chars block reference
        const unique = uuid.substring(6, 10);      // 4 chars unique
        
        return {
            id: `TBO-BLK-${blockRef}-${unique}`,
            blockRef: blockRef,
            full: `TBO-BLK-${blockRef}-${unique}`
        };
        // Example: TBO-BLK-1A2B3C-4D5E
    },
    
    // Generate GP Ward ID
    generateGpWardId: (blockRef, gpWardName) => {
        const uuid = uuidv4().replace(/-/g, '');
        const gpWardUnique = uuid.substring(0, 6); // 6 chars unique to gp ward
        
        return {
            id: `TBO-GPW-${blockRef}-${gpWardUnique}`,
            blockRef: blockRef,
            gpWardRef: `${blockRef}-${gpWardUnique}`,
            full: `TBO-GPW-${blockRef}-${gpWardUnique}`
        };
        // Example: TBO-GPW-1A2B3C-9XYZ56
    },
    
    // Generate Village ID (depends on BOTH block AND gp ward)
    generateVillageId: (blockRef, gpWardUnique, villageName) => {
        const uuid = uuidv4().replace(/-/g, '');
        const villageUnique = uuid.substring(0, 4); // 4 chars unique to village
        
        return {
            id: `TBO-VIL-${blockRef}-${gpWardUnique}-${villageUnique}`,
            blockRef: blockRef,
            gpWardRef: `${blockRef}-${gpWardUnique}`,
            full: `TBO-VIL-${blockRef}-${gpWardUnique}-${villageUnique}`
        };
        // Example: TBO-VIL-1A2B3C-9XYZ56-ABCD
    },
    
    // Compact version (without separators but readable chunks)
    generateCompactId: (type, parts) => {
        // type: 'BLK', 'GPW', 'VIL'
        // parts: array of strings
        return `TBO${type}${parts.join('')}`;
        // TBOBLK1A2B3C4D5E (no separators but fixed length chunks)
    },
    
    // Parse any ID to get its components
    parseId: (id) => {
        // Handle both formats: with or without separators
        const parts = id.split('-');
        
        if (parts.length === 4) { // TBO-BLK-1A2B3C-4D5E
            const [tbo, level, ref, unique] = parts;
            return {
                full: id,
                level,
                ref,
                unique,
                blockRef: level === 'BLK' ? ref : (level === 'GPW' ? ref : ref),
                gpWardRef: level === 'VIL' ? `${ref}-${unique.substring(0, 6)}` : null
            };
        }
        else if (parts.length === 5) { // TBO-VIL-1A2B3C-9XYZ56-ABCD
            const [tbo, level, blockRef, gpWardUnique, villageUnique] = parts;
            return {
                full: id,
                level,
                blockRef,
                gpWardUnique,
                villageUnique,
                gpWardRef: `${blockRef}-${gpWardUnique}`
            };
        }
        
        return null;
    },
    
    // Even more compact but still readable (Base36)
    generateBase36Id: (type, name) => {
        // Use timestamp in base36 + random
        const timestamp = Date.now().toString(36).substring(4); // Last 4 chars
        const random = Math.random().toString(36).substring(2, 6); // 4 chars
        
        return `TBO${type}${timestamp}${random}`;
        // TBOBLK1a2b3c4d (10 chars total after TBO)
    }
};