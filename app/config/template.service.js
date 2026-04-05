const fs = require('fs');
const path = require('path');
const fontPath = path.resolve(__dirname, "../../public/assets/fonts/TiroDevanagariHindi-Regular.ttf");
const fontBase64 = fs.readFileSync(fontPath).toString('base64');

const formatDate = (date) => {
    if (!date) return "";
    const d = new Date(date);
    if (isNaN(d)) return "";
    return d.toLocaleDateString("hi-IN");
};

const escapeHtml = (text) => {
    if (!text) return "";
    return String(text)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
};

const generatePDFHTML = (data) => {
    if (!data || data.length === 0) return "<h2>No Data Found</h2>";

    const first = data[0];

    // ===== DYNAMIC SECTION TABLE =====
    const uniqueSections = [...new Set(data.map(d => d.section).filter(Boolean))];

    // console.log('uniqu sction ->>>>>> ', uniqueSections)


    // process.exit(1)
    const sectionChunks = [];
    for (let i = 0; i < uniqueSections.length; i += 10) {
        sectionChunks.push(uniqueSections.slice(i, i + 10));
    }

    if (sectionChunks.length === 0) {
        sectionChunks.push([]);
    }

    const sectionTablesHTML = sectionChunks.map((chunk, chunkIndex) => {

        // console.log('chunk ->>>> ', chunk)

        let rowsHTML = "";

        chunk.forEach((section, index) => {
            rowsHTML += `
<tr>
  <td colspan="3" style="
    border:1px solid #000;
    padding:6px;
    height:28px;
    font-size:14px;
    text-align:left;
    vertical-align:left;
  ">
    ${chunkIndex * 10 + index + 1}. ${section}
  </td>
</tr>
`;
        });

        return `
<table style="
  width:100%;
  border-collapse:collapse;
  font-size:12px;
  table-layout:fixed;
  border:1px solid #000;
  margin-bottom:20px;
">

<tr style="background-color:#e6e6e6;">
<td colspan="3" style="
  border:1px solid #000;
  padding:6px;
  font-weight:bold;
  text-align:left;
  font-size:14px;
  height:26px;
">
    ग्राम/वार्ड में आने वाले क्षेत्र
  </td>
</tr>

${rowsHTML}

</table>

${chunkIndex < sectionChunks.length - 1
                ? '<div style="page-break-after: always;"></div>'
                : ''
            }
`;
    }).join("");

    const families = {};
    const seenVoters = new Set(); // Track unique voters

    data.forEach(member => {
        // Create a unique key for each voter (vsno + familyid + section)
        const voterKey = `${member.vsno}_${member.familyid}_${member.section}`;

        // Skip if we've already seen this voter
        if (seenVoters.has(voterKey)) {
            return; // Skip this duplicate
        }

        // Mark as seen
        seenVoters.add(voterKey);

        // Now add to families grouping
        const section = member.section || 'nosection';
        const uniqueKey = `${member.familyid}_${section}`;

        if (!families[uniqueKey]) {
            families[uniqueKey] = [];
        }
        families[uniqueKey].push(member);
    });

    // Check for duplicates
    const seen = new Set();
    const duplicates = [];
    data.forEach(member => {
        const key = `${member.vsno}_${member.familyid}_${member.section}`;
        if (seen.has(key)) {
            duplicates.push(member);
        } else {
            seen.add(key);
        }
    });

    // Check if same familyid appears in multiple sections
    const familySections = {};
    data.forEach(member => {
        if (!familySections[member.familyid]) {
            familySections[member.familyid] = new Set();
        }
        familySections[member.familyid].add(member.section);
    });

    Object.entries(familySections).forEach(([familyId, sections]) => {
        if (sections.size > 1) {
        }
    });
    // ===== END DEBUG CODE =====

    // First, get all unique sections and sort them by the MINIMUM vsno in that section
    const sectionsWithMinVsno = {};

    Object.keys(families).forEach(key => {
        const [familyId, section] = key.split('_');
        if (!sectionsWithMinVsno[section]) {
            // Find minimum vsno in this section across all families
            const minVsno = Math.min(...families[key].map(m => parseInt(m.vsno) || 999999));
            sectionsWithMinVsno[section] = minVsno;
        }
    });

    // Sort sections by their minimum vsno (this will put sections in correct vsno order)
    const sortedSections = Object.keys(sectionsWithMinVsno).sort((a, b) => {
        return (sectionsWithMinVsno[a] || 0) - (sectionsWithMinVsno[b] || 0);
    });

    // Now create sortedFamilyKeys based on section order and then by minimum vsno within section
    const sortedFamilyKeys = [];

    sortedSections.forEach(section => {
        // Get all families in this section
        const familiesInSection = Object.keys(families)
            .filter(key => key.endsWith(`_${section}`))
            .sort((a, b) => {
                // Sort families by their minimum vsno
                const minVsnoA = Math.min(...families[a].map(m => parseInt(m.vsno) || 999999));
                const minVsnoB = Math.min(...families[b].map(m => parseInt(m.vsno) || 999999));
                return minVsnoA - minVsnoB;
            });

        sortedFamilyKeys.push(...familiesInSection);
    });

    // Process families WITHOUT chunking - keep all members together
    //     const familyPages = Object.values(families).map((family, index) => {
    //         const head = family[0];

    //         const memberRows = family
    //             .map(
    //                 (m, i) => `
    //         <tr>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${m.vsno}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:left; white-space: normal; word-break: break-word;">${m.vname || ""}${m.sex && m.age ? ` (${m.sex}/${m.age})` : ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:left; white-space: normal; word-break: break-word;">${m.rname || ""} ${m.rname ? `(${m.relation})` : ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${formatDate(m.dob)}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${m.phone1 || ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${m.edu_id || ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center; white-space: normal; word-break: break-word;">${m.proff_id || ""}</td>
    //         </tr>
    //       `
    //             )
    //             .join("");

    //         return `
    // <div class="page family-page" style="padding: 15mm 10mm 15mm 20mm; display: flex; flex-direction: column; min-height: 1120px; box-sizing: border-box;">

    //     <div style="flex: 1;">
    //         <div style="text-align: center; margin-bottom: 2px; font-size: 18pt; font-weight: bold; width: 100%; max-width: 100%; padding: 0; margin: 0 0 2px 0;">भारतीय सामाजिक परिवार सुरक्षा / लाभार्थी चिन्हीकरण-दौनो</div>

    //         <p style="text-align:center; margin-top:-4px; margin-bottom:10px; font-size:16px;">
    //             वार्ड / ग्राम - ${head.section || ""}
    //         </p>

    //         <p style="font-size:14px; margin-top: -7px">
    //             परिवार संख्या - ${index + 1}
    //             &nbsp;&nbsp;&nbsp;
    //             मकान नंबर - ${head.hno || ""}
    //             <span style="float:right;">क्षेत्र - ${head.section || ""}</span>
    //         </p>

    //         <table style="width:100%; border-collapse:collapse; margin-bottom:20px; border:1px solid #000; margin-top: -14px">
    //             <tr style="background-color: #E0E0E0">
    //           <tr>
    //             <th style="width: 6%; background-color: #e0e0e0;">क्र.स.</th>
    //             <th style="width: 17%; background-color: #e0e0e0;">सदस्य का नाम</th>
    //             <th style="width: 17%; background-color: #e0e0e0;">पिता / पति का नाम</th>
    //             <th style="width: 10%; background-color: #e0e0e0;">जन्मदिवस</th>
    //             <th style="width: 15%; background-color: #e0e0e0;">मोबाइल</th>
    //             <th style="width: 7%; background-color: #e0e0e0;">शिक्षा</th>
    //             <th style="width: 21%; border-right: 1px solid #000; background-color: #e0e0e0; font-size: 9pt;">पेशा (किसान/गृहणी/सरकारी/गैर सरकारी) / व्यवसाय एवं अन्य जानकारी</th>
    //           </tr>
    //             </tr>
    //             ${memberRows}
    //         </table>

    //         <!-- ===== CASTE SECTION ===== -->
    // <p style="font-size:14px; margin-top: -10px; line-height: 1.2;">
    //     <span style="display: inline-flex; align-items: center; gap: 8px;">
    //         <span>ST ${head.cast_cat === "ST" ? "☑" : "☐"}</span>
    //         <span>SC ${head.cast_cat === "SC" ? "☑" : "☐"}</span>
    //         <span>OBC ${head.cast_cat === "O" ? "☑" : "☐"}</span>
    //         <span>GEN ${head.cast_cat === "GEN" ? "☑" : "☐"}</span>
    //     </span>

    //     <span style="margin-left:50px;">
    //         परिवार का सामाजिक वर्ग - ${head.cast_cat || ""}
    //     </span>

    //     <span style="margin-left: 100px;">
    //         उपवर्ग - ......................
    //     </span>
    // </p>

    //         <!-- ===== SCHEME SECTION ===== -->
    //         <div style="font-size:14px; line-height:1.5; margin-top: -10px">
    //             <p style="text-align:center; font-weight:normal; margin-top: -10px;">
    //                 निम्नलिखित योजनाओं में से परिवार को जिन योजनाओं का लाभ मिल रहा है उस पर सही अंकित करें :
    //             </p>

    //             <div style="display:flex; justify-content:space-between; padding:0 10px; margin-top: -13px">
    //                 <span>☐ सामाजिक पेंशन</span>
    //                 <span>☐ किसान सम्मान निधि</span>
    //                 <span>☐ आयुष्मान कार्ड</span>
    //                 <span>☐ उज्ज्वला योजना</span>
    //                 <span>☐ खाद्य सुरक्षा</span>
    //             </div>

    //             <p style="text-align:center; margin-top: -5px">
    //                 यदि परिवार में कोई व्यक्ति सामाजिक सुरक्षा, वृद्धावस्था (महिला 55 वर्ष, पुरुष 58 वर्ष), विधवा, विकलांग पेंशन के पात्र हों व उन्हें पेंशन नहीं मिलती हो तो उनका नाम पृथक से लिखें:
    //             </p>

    //             <div style="display:flex; justify-content:space-between; padding:0 30px; margin-top: -10px">
    //                 <span>1. ..................................................</span>
    //                 <span>2. ..................................................</span>
    //                 <span>3. ..................................................</span>
    //             </div>
    //         </div>
    //     </div>

    //     <!-- FOOTER - ALWAYS AT BOTTOM -->
    //     <div style="margin-top: auto; width:100%;">
    //         <footer style="display: flex; justify-content: space-between; align-items: center; width:100%; padding-top:20px;">
    //             <p style="font-size: 10px; margin:0;">${head.bhag_no || ""}</p>
    //             <p style="font-size: 12px; margin:0;">Page ${index + 1} of ${Object.keys(families).length + 1}</p>
    //             <p style="font-size: 10px; margin:0;">${head.ac_no || ""}/${head.data_id || ""}</p>
    //         </footer>
    //     </div>

    // </div>
    // `;
    //     }).join("");

    // ===== Process families to allow multiple per page =====
    //     const familyPages = (() => {
    //         const pages = [];
    //         let currentPageContent = '';
    //         let currentHeight = 0;
    //         const pageHeight = 1120;
    //         const footerHeight = 40;
    //         const usableHeight = pageHeight - footerHeight;
    //         let lastSection = null;
    //         let isFirstFamilyPage = true;

    //         sortedFamilyKeys.forEach((key, index) => {
    //             const family = families[key];
    //             const [familyid, section] = key.split('_'); // You can use these if needed
    //             const head = family[0];

    //             const memberRows = family
    //                 .map(
    //                     (m, i) => `
    //         <tr>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${m.vsno}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:left; white-space: normal; word-break: break-word;">${m.vname || ""}${m.sex && m.age ? ` (${m.sex}/${m.age})` : ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:left; white-space: normal; word-break: break-word;">${m.rname || ""} ${m.rname ? `(${m.relation})` : ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${formatDate(m.dob)}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${m.phone1 || ""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center;">${""}</td>
    //             <td style="border:1px solid #000; padding:4px; text-align:center; white-space: normal; word-break: break-word;">${m.proff_id || ""}</td>
    //         </tr>`
    //                 ).join("");

    //             const familyHTML = `

    //     <p style="font-size:13px; margin-top:-3px; margin-bottom:12px;">
    //         <span style="font-weight: 800">परिवार संख्या - ${index + 1}</span>
    //         &nbsp;&nbsp;&nbsp;
    //         मकान नंबर - ${head.hno || ""}
    //         <span style="float:right;">क्षेत्र - ${head.section || ""}</span>
    //     </p>

    //     <table style="width:100%; border-collapse:collapse; margin-bottom:5px; border:1px solid #000; margin-top: -13px">
    //         <tr style="background-color: #E0E0E0">
    //             <th style="width: 6%; background-color: #e0e0e0;">क्र.स.</th>
    //             <th style="width: 17%; background-color: #e0e0e0;">सदस्य का नाम</th>
    //             <th style="width: 17%; background-color: #e0e0e0;">पिता / पति का नाम</th>
    //             <th style="width: 10%; background-color: #e0e0e0;">जन्मदिवस</th>
    //             <th style="width: 15%; background-color: #e0e0e0;">मोबाइल</th>
    //             <th style="width: 7%; background-color: #e0e0e0;">शिक्षा</th>
    //             <th style="width: 21%; border-right: 1px solid #000; background-color: #e0e0e0; font-size: 8pt;">पेशा/व्यवसाय/जाति/अन्य जानकारी</th>
    //         </tr>
    //         ${memberRows}
    //     </table>

    //     <p style="font-size:13px; margin-top: 5px; line-height: 1.2;">
    //         <span style="display: inline-flex; align-items: center; gap: 8px;">
    //             <span>ST ${head.cast_cat === "ST" ? "☑" : "☐"}</span>
    //             <span>SC ${head.cast_cat === "SC" ? "☑" : "☐"}</span>
    //             <span>OBC ${head.cast_cat === "OBC" ? "☑" : "☐"}</span>
    //             <span>GEN ${head.cast_cat === "GEN" ? "☑" : "☐"}</span>
    //         </span>

    //         <span style="margin-left:70px;">
    //             परिवार का सामाजिक वर्ग - 
    //         </span>

    //         <span style="margin-left: 150px;">
    //             उपजाति - 
    //         </span>
    //     </p>

    //     <!-- ===== SCHEME SECTION ===== -->
    //     <div style="font-size:13px; line-height:1.5; margin-top: 3px; margin-bottom: 10px;">
    //         <p style="text-align:center; font-weight:normal; margin-top: -9px;">
    //             निम्नलिखित योजनाओं में से परिवार को जिन योजनाओं का लाभ मिल रहा है उस पर सही अंकित करें :
    //         </p>

    //         <div style="display:flex; justify-content:space-between; padding:0 10px; margin-top: -13px;">
    //             <span style="font-weight: 800">☐ सामाजिक पेंशन</span>
    //             <span style="font-weight: 800">☐ किसान सम्मान निधि</span>
    //             <span style="font-weight: 800">☐ आयुष्मान कार्ड</span>
    //             <span style="font-weight: 800">☐ उज्ज्वला योजना</span>
    //             <span style="font-weight: 800">☐ खाद्य सुरक्षा</span>
    //         </div>

    //         <p style="text-align:center; margin-top: -0.2px">
    //             यदि परिवार में कोई व्यक्ति सामाजिक सुरक्षा, वृद्धावस्था (महिला 55 वर्ष, पुरुष 58 वर्ष), विधवा, विकलांग <br/>पेंशन के पात्र हों व उन्हें पेंशन नहीं मिलती हो तो उनका नाम पृथक से लिखें:
    //         </p>

    //         <div style="display:flex; justify-content:space-between; padding:0 25px; margin-top: -14px">
    //             <span>1. ......................................</span>
    //             <span>2. ......................................</span>
    //             <span>3. ......................................</span>
    //         </div>
    //     </div>
    // `;

    //             // Rough height estimation
    //             const estimatedHeight = 150 + family.length * 28 + 120; // header + table + scheme section

    //             if (currentHeight + estimatedHeight > usableHeight) {
    //                 // push last page with footer
    //                 // push last page with footer AND HEADING
    //                 // push last page with footer
    //                 if (currentPageContent) {
    //                     const head = Object.values(families)[Object.values(families).length - 1][0];

    //                     // ===== DEBUG CONSOLE FOR LAST PAGE =====
    //                     console.log("===== LAST PAGE DEBUG =====");
    //                     console.log("currentPageContent exists:", !!currentPageContent);
    //                     console.log("currentPageContent length:", currentPageContent.length);
    //                     console.log("pages array length before push:", pages.length);
    //                     console.log("Total families:", Object.keys(families).length);
    //                     console.log("Last family head:", {
    //                         gp_ward: head?.gp_ward,
    //                         village: head?.village,
    //                         bhag_no: head?.bhag_no,
    //                         ac_no: head?.ac_no,
    //                         data_id: head?.data_id
    //                     });
    //                     console.log("Heading to be added:", `भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण - ${head?.gp_ward || ''}`);
    //                     console.log("===========================");
    //                     // ===== END DEBUG =====

    //                     pages.push(`
    // <div class="page family-page" style="padding:10mm 15mm 15mm 15mm; display:flex; flex-direction:column; min-height:1120px;">
    //     <div style="text-align: center; font-size: 15pt; margin-bottom: 15px; font-weight: bold;">
    //         भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण - ${head.gp_ward || ''}
    //     </div>
    //     ${currentPageContent}
    //     <div style="margin-top: auto; width:100%;">
    //         <footer style="display: flex; justify-content: space-between; align-items: center; width:100%; padding-top:10px;">
    //             <p style="font-size: 4.5px; margin:0;">${head.bhag_no || ""}</p>
    //             <p style="font-size: 11px; margin:0;">Page ${pages.length + 1} of ${Object.keys(families).length}</p>
    //             <p style="font-size: 4.5px; margin:0;">${head.ac_no || ""}/${head.data_id || ""}</p>
    //         </footer>
    //     </div>
    // </div>`);

    //                     // Debug after push
    //                     console.log("pages array length after push:", pages.length);
    //                 }
    //                 currentPageContent = '';
    //                 currentHeight = 0;
    //                 lastSection = null;
    //             }

    //             currentPageContent += familyHTML;
    //             currentHeight += estimatedHeight;
    //             lastSection = head.section;
    //             if (isFirstFamilyPage) {
    //                 isFirstFamilyPage = false;
    //             }
    //         });

    //         // push last page with footer
    //         if (currentPageContent) {
    //             const head = Object.values(families)[Object.values(families).length - 1][0];
    //             pages.push(`
    // <div class="page family-page" style="padding:10mm 15mm 15mm 15mm; display:flex; flex-direction:column; min-height:1120px;">
    //     ${currentPageContent}
    //     <div style="margin-top: auto; width:100%;">
    //         <footer style="display: flex; justify-content: space-between; align-items: center; width:100%; padding-top:10px;">
    //             <p style="font-size: 4.5px; margin:0;">${head.bhag_no || ""}</p>
    //             <p style="font-size: 11px; margin:0;">Page ${pages.length + 1} of ${Object.keys(families).length}</p>
    //             <p style="font-size: 4.5px; margin:0;">${head.ac_no || ""}/${head.data_id || ""}</p>
    //         </footer>
    //     </div>
    // </div>`);
    //         }

    //         return pages.join('');
    //     })();

    // ===== Process families to allow multiple per page =====
    const familyPages = (() => {
        const pages = [];
        let currentPageContent = '';
        let currentHeight = 0;
        const pageHeight = 1120;
        const footerHeight = 40;
        const usableHeight = pageHeight - footerHeight;
        let lastSection = null;
        let isFirstFamilyPage = true;

        // First, calculate how many pages we'll need
        const pageEstimates = [];
        let tempContent = '';
        let tempHeight = 0;

        sortedFamilyKeys.forEach((key) => {
            const family = families[key];
            const estimatedHeight = 150 + family.length * 28 + 120;

            if (tempHeight + estimatedHeight > usableHeight) {
                pageEstimates.push({ content: tempContent, height: tempHeight });
                tempContent = '';
                tempHeight = 0;
            }

            tempContent += 'family'; // placeholder
            tempHeight += estimatedHeight;
        });

        if (tempHeight > 0) {
            pageEstimates.push({ content: tempContent, height: tempHeight });
        }

        const totalPages = pageEstimates.length;

        // Now generate actual pages with correct numbering
        sortedFamilyKeys.forEach((key, index) => {
            const family = families[key];
            const [familyid, section] = key.split('_');
            const head = family[0];

            const memberRows = family
                .map(
                    (m, i) => `
        <tr>
            <td style="border:1px solid #000; padding:4px; text-align:center;">${m.vsno}</td>
            <td style="border:1px solid #000; padding:4px; text-align:left; white-space: normal; word-break: break-word;">${m.vname || ""}${m.sex && m.age ? ` (${m.sex}/${m.age})` : ""}</td>
            <td style="border:1px solid #000; padding:4px; text-align:left; white-space: normal; word-break: break-word;">${m.rname || ""} ${m.rname ? `(${m.relation})` : ""}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">${formatDate(m.dob)}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">${m.phone1 || ""}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">${""}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center; white-space: normal; word-break: break-word;">${m.proff_id || ""}</td>
        </tr>`
                ).join("");

            const familyHTML = `

    <p style="font-size:13px; margin-top:-3px; margin-bottom:12px;">
        <span style="font-weight: 800">परिवार संख्या - ${index + 1}</span>
        &nbsp;&nbsp;&nbsp;
        मकान नंबर - ${head.hno || ""}
        <span style="float:right;">क्षेत्र - ${head.section || ""}</span>
    </p>

    <table style="width:100%; border-collapse:collapse; margin-bottom:5px; border:1px solid #000; margin-top: -13px">
        <tr style="background-color: #E0E0E0">
            <th style="width: 6%; background-color: #e0e0e0;">क्र.स.</th>
            <th style="width: 17%; background-color: #e0e0e0;">सदस्य का नाम</th>
            <th style="width: 17%; background-color: #e0e0e0;">पिता / पति का नाम</th>
            <th style="width: 10%; background-color: #e0e0e0;">जन्मदिवस</th>
            <th style="width: 15%; background-color: #e0e0e0;">मोबाइल</th>
            <th style="width: 7%; background-color: #e0e0e0;">शिक्षा</th>
            <th style="width: 21%; border-right: 1px solid #000; background-color: #e0e0e0; font-size: 8pt;">पेशा/व्यवसाय/जाति/अन्य जानकारी</th>
        </tr>
        ${memberRows}
    </table>

    <p style="font-size:13px; margin-top: 5px; line-height: 1.2;">
        <span style="display: inline-flex; align-items: center; gap: 8px;">
            <span>ST ${head.cast_cat === "ST" ? "☑" : "☐"}</span>
            <span>SC ${head.cast_cat === "SC" ? "☑" : "☐"}</span>
            <span>OBC ${head.cast_cat === "OBC" ? "☑" : "☐"}</span>
            <span>GEN ${head.cast_cat === "GEN" ? "☑" : "☐"}</span>
        </span>

        <span style="margin-left:70px;">
            परिवार का सामाजिक वर्ग - 
        </span>

        <span style="margin-left: 150px;">
            उपजाति - 
        </span>
    </p>

    <!-- ===== SCHEME SECTION ===== -->
    <div style="font-size:13px; line-height:1.5; margin-top: 3px; margin-bottom: 10px;">
        <p style="text-align:center; font-weight:normal; margin-top: -9px;">
            निम्नलिखित योजनाओं में से परिवार को जिन योजनाओं का लाभ मिल रहा है उस पर सही अंकित करें :
        </p>

        <div style="display:flex; justify-content:space-between; padding:0 10px; margin-top: -13px;">
            <span style="font-weight: 800">☐ सामाजिक पेंशन</span>
            <span style="font-weight: 800">☐ किसान सम्मान निधि</span>
            <span style="font-weight: 800">☐ आयुष्मान कार्ड</span>
            <span style="font-weight: 800">☐ उज्ज्वला योजना</span>
            <span style="font-weight: 800">☐ खाद्य सुरक्षा</span>
        </div>

        <p style="text-align:center; margin-top: -0.2px">
            यदि परिवार में कोई व्यक्ति सामाजिक सुरक्षा, वृद्धावस्था (महिला 55 वर्ष, पुरुष 58 वर्ष), विधवा, विकलांग <br/>पेंशन के पात्र हों व उन्हें पेंशन नहीं मिलती हो तो उनका नाम पृथक से लिखें:
        </p>

        <div style="display:flex; justify-content:space-between; padding:0 25px; margin-top: -14px">
            <span>1. ......................................</span>
            <span>2. ......................................</span>
            <span>3. ......................................</span>
        </div>
    </div>
`;

            const estimatedHeight = 150 + family.length * 28 + 120;

            if (currentHeight + estimatedHeight > usableHeight) {
                if (currentPageContent) {
                    const remainingSpace = usableHeight - currentHeight;
                    const footerMargin = remainingSpace > 60 ? 'auto' : '7px';

                    pages.push(`
                        <div class="page family-page" style="padding:10mm 15mm 15mm 15mm; display:flex; flex-direction:column; min-height:1120px;">
                        <div style="text-align: center; font-size: 15pt; margin-bottom: 8px;">
                            भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण - ${head.gp_ward}
                        </div>
                        <p style="text-align:center; margin-top: -5px; margin-bottom:8px; font-size:16px;">
                            वार्ड / ग्राम - ${head.village || ""}
                        </p>

                    ${currentPageContent}
                        <div style="margin-top:${footerMargin}; width:100%;">
                            <footer style="display: flex; justify-content: space-between; align-items: center; width:100%; padding-top:20px;">
                                <p style="font-size: 9px; margin:0;">${head.bhag_no || ""}</p>
                                <p style="font-size: 11px; margin:0;">Page ${pages.length + 1} of ${totalPages}</p>
                                <p style="font-size: 9px; margin:0;">${head.ac_no || ""}/${head.data_id || ""}</p>
                            </footer>
                        </div>
                    </div>
                `);
                }
                currentPageContent = '';
                currentHeight = 0;
                lastSection = null;
            }

            currentPageContent += familyHTML;
            currentHeight += estimatedHeight;
            lastSection = head.section;
            if (isFirstFamilyPage) {
                isFirstFamilyPage = false;
            }
        });

        // push last page with footer
        if (currentPageContent) {
            const head = Object.values(families)[Object.values(families).length - 1][0];

            pages.push(`
<div class="page family-page" style="padding:10mm 15mm 15mm 15mm; display:flex; flex-direction:column; min-height:1120px;">
    <div style="text-align: center; font-size: 15pt; margin-bottom: 15px;">
        भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण - ${head.gp_ward || ''}
    </div>
    ${currentPageContent}
    <div style="margin-top: auto; width:100%;">
        <footer style="display: flex; justify-content: space-between; align-items: center; width:100%; padding-top:10px;">
            <p style="font-size: 9px; margin:0;">${head.bhag_no || ""}</p>
            <p style="font-size: 11px; margin:0;">Page ${pages.length + 1} of ${totalPages}</p>
            <p style="font-size: 9px; margin:0;">${head.ac_no || ""}/${head.data_id || ""}</p>
        </footer>
    </div>
</div>`);
        }

        return pages.join('');
    })();

    // Get all unique sections for the first page
    const allSections = [...new Set(data.map(d => d.section).filter(Boolean))];

    // Generate up to 5 rows for sections on first page
    const sectionRows = Array.from({ length: 5 }, (_, index) => {
        const sectionName = allSections[index] || '';
        return `
            <tr>
                <td style="border: 1px solid #000; border-top: none; padding: 8px 15px; text-align: left; font-size: 12pt; min-height: 40px; height: 40px; vertical-align: middle;">${index + 1}. ${escapeHtml(sectionName)}</td>
            </tr>`;
    }).join('');

    return `
<!DOCTYPE html>
<html lang="hi">
<head>
<meta charset="UTF-8">

<title>भारतीय सामाजिक परिवार सुरक्षा</title>

<style>
@font-face {
        font-family: 'TiroDevanagari';
        src: url(data:font/ttf;charset=utf-8;base64,${fontBase64}) format('truetype');
        font-weight: normal;
        font-style: normal;
    }
body {
    font-family: 'TiroDevanagari', serif;
    background-color: #ccc;
    padding: 8px 0;
    margin: 0;
}

.page-first, .family-page {
    width: 800px;
    height: 1120px;
    background: #fff;
    margin: 20px auto;
    box-sizing: border-box;
    position: relative;
    page-break-after: always;
}

.page-first {
    padding: 10mm 20mm 12mm 20mm; /* 20mm left margin for binding */
    display: flex;
    flex-direction: column;
}

.family-page {
    padding: 10mm 20mm 12mm 20mm; /* 20mm left margin for binding */
    display: flex;
    flex-direction: column;
}

h2 {
    text-align: center;
    margin-bottom: 20px;
    font-weight: bold;
    font-size: 18px;
}

table {
    width: 100%;
    border-collapse: collapse;
    border: 1px solid #000;
}

th, td {
    border: 1px solid #000;
    padding: 4px;
    font-size: 13px;
    vertical-align: middle;
}

th {
    background-color: #e0e0e0;
    font-weight: bold;
    text-align: center;
}

.footer {
    text-align: center;
    font-weight: bold;
}

@media print {
    body { background: none; padding: 0; }
    .page-first, .family-page { 
        margin: 0 auto; 
        page-break-after: always; 
        box-shadow: none;
    }
}
</style>
</head>

<body>

<!-- FIRST PAGE - WITH LEFT MARGIN FOR BINDING AND FOOTER AT BOTTOM -->
<div class="page-first" style="display: flex; flex-direction: column; min-height: 1120px;">

    <div style="flex: 1;">
        <div style="text-align: center; font-size: 19pt; font-weight: bold; margin-bottom: 20px; line-height: 1.4;">
            ।। मेरा भारत - विकसित भारत।।  <br/>  
            भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण
        </div>
        
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 0; border: 1px solid #000;">
            <thead>
                <tr>
                    <th style="border: 1px solid #000; padding: 10px 8px; background-color: #e0e0e0; text-align: center; width: 33.33%; font-weight: bold; font-size: 12pt;">पंचायत समिति / तहसील</th>
                    <th style="border: 1px solid #000; padding: 10px 8px; background-color: #e0e0e0; text-align: center; width: 33.33%; font-weight: bold; font-size: 12pt;">ग्राम पंचायत / वार्ड</th>
                    <th style="border: 1px solid #000; padding: 10px 8px; background-color: #e0e0e0; text-align: center; width: 33.33%; font-weight: bold; font-size: 12pt;">ग्राम / नगर पालिका</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="border: 1px solid #000; padding: 10px 8px; text-align: center; font-size: 12pt; min-height: 30px; vertical-align: middle;">${escapeHtml(first.block || '')}</td>
                    <td style="border: 1px solid #000; padding: 10px 8px; text-align: center; font-size: 12pt; min-height: 30px; vertical-align: middle;">${escapeHtml(first.gp_ward || '')}</td>
                    <td style="border: 1px solid #000; padding: 10px 8px; text-align: center; font-size: 12pt; min-height: 30px; vertical-align: middle;">${escapeHtml(first.village || '')}</td>
                </tr>
                <tr>
                    <td colspan="3" style="border: 1px solid #000; border-top: 1px solid #000; padding: 10px 8px; text-align: left; font-size: 12pt; font-weight: bold; background-color: #e0e0e0;">
                        ग्राम/वार्ड में आने वाले क्षेत्र
                    </td>
                </tr>
            </tbody>
        </table>
        
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px; border: 1px solid #000; border-top: none;">
            <tbody>
                ${sectionRows}
            </tbody>
        </table>
    </div>
    
    <!-- FOOTER AT VERY BOTTOM OF FIRST PAGE -->
    <div style="margin-top: auto; width:100%;">
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #000;">
            <thead>
                <tr>
                    <th style="border: 1px solid #000; padding: 10px 8px; background-color: #fff; text-align: center; width: 33.33%; font-weight: bold; font-size: 12pt;">केंद्र का नाम व पता</th>
                    <th style="border: 1px solid #000; padding: 10px 8px; background-color: #fff; text-align: center; width: 33.33%; font-weight: bold; font-size: 12pt;">सर्वे कर्ता का नाम व मोबाइल नं.</th>
                    <th style="border: 1px solid #000; padding: 10px 8px; background-color: #fff; text-align: center; width: 33.33%; font-weight: bold; font-size: 12pt;">सर्वे कर्ता का नाम व मोबाइल नं.</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="border: 1px solid #000; padding: 14px 8px; text-align: center; font-size: 12pt; min-height: 90px; height: 90px;"></td>
                    <td style="border: 1px solid #000; padding: 14px 8px; text-align: center; font-size: 12pt; min-height: 70px; height: 70px;"></td>
                    <td style="border: 1px solid #000; padding: 14px 8px; text-align: center; font-size: 12pt; min-height: 70px; height: 70px;"></td>
                </tr>
            </tbody>
        </table>
        
        <div style="text-align: center; font-size: 12pt; font-weight: bold; padding-top: 10px;">
            बुकलेट संख्या - ${escapeHtml(String(first?.bhag_no || ''))}
        </div>
    </div>
</div>
<div class="family-page" style="display: flex; flex-direction: column; min-height: 1120px;">
</div>
${familyPages}

</body>
</html>
`;
};

const generateBlankRegisterHTML = () => {

    // ===== BLANK SECTION ROWS (First Page) =====
    const sectionRows = Array.from({ length: 5 }, (_, index) => `
        <tr>
            <td style="border: 1px solid #000; border-top: none; padding: 8px 15px; text-align: left; font-size: 12pt; min-height: 40px; height: 40px; vertical-align: middle;">
                ${index + 1}.
            </td>
        </tr>
    `).join('');

    return `
<!DOCTYPE html>
<html lang="hi">
<head>
<meta charset="UTF-8">
<title>भारतीय सामाजिक परिवार सुरक्षा</title>

<style>
@font-face {
    font-family: 'TiroDevanagari';
    src: url(data:font/ttf;charset=utf-8;base64,${fontBase64}) format('truetype');
    font-weight: normal;
    font-style: normal;
}

body {
    font-family: 'TiroDevanagari', serif;
    background-color: #ccc;
    padding: 8px 0;
    margin: 0;
}

.page-first, .family-page {
    width: 800px;
    height: 1120px;
    background: #fff;
    margin: 20px auto;
    box-sizing: border-box;
    position: relative;
    page-break-after: always;
}

.page-first {
    padding: 10mm 20mm 12mm 20mm;
    display: flex;
    flex-direction: column;
}

.family-page {
    padding: 10mm 20mm 12mm 20mm;
    display: flex;
    flex-direction: column;
}

table {
    width: 100%;
    border-collapse: collapse;
    border: 1px solid #000;
}

th, td {
    border: 1px solid #000;
    padding: 4px;
    font-size: 13px;
    vertical-align: middle;
}

th {
    background-color: #e0e0e0;
    font-weight: bold;
    text-align: center;
}

@media print {
    body { background: none; padding: 0; }
    .page-first, .family-page { 
        margin: 0 auto; 
        page-break-after: always; 
        box-shadow: none;
    }
}
</style>
</head>

<body>

<!-- ================= FIRST PAGE ================= -->

<div class="page-first" style="display: flex; flex-direction: column; min-height: 1120px;">

    <div style="flex: 1;">
        <div style="text-align: center; font-size: 19pt; font-weight: bold; margin-bottom: 20px; line-height: 1.4;">
            भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण
        </div>
        
        <table style="margin-bottom: 0;">
            <thead>
                <tr>
                    <th style="width: 33.33%; font-size: 12pt;">पंचायत समिति / तहसील</th>
                    <th style="width: 33.33%; font-size: 12pt;">ग्राम पंचायत / वार्ड</th>
                    <th style="width: 33.33%; font-size: 12pt;">ग्राम / नगर पालिका</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="height:40px;"></td>
                    <td></td>
                    <td></td>
                </tr>
                <tr>
                    <td colspan="3" style="text-align: left; font-size: 12pt; font-weight: bold;">
                        ग्राम/वार्ड में आने वाले क्षेत्र
                    </td>
                </tr>
            </tbody>
        </table>
        
        <table style="margin-bottom: 30px; border-top: none;">
            <tbody>
                ${sectionRows}
            </tbody>
        </table>
    </div>
    
    <div style="margin-top: auto; width:100%;">
        <table style="margin-bottom: 15px;">
            <thead>
                <tr>
                    <th style="width: 33.33%; font-size: 12pt; background-color:#fff;">केंद्र का नाम व पता</th>
                    <th style="width: 33.33%; font-size: 12pt; background-color:#fff;">सर्वे कर्ता का नाम व मोबाइल नं.</th>
                    <th style="width: 33.33%; font-size: 12pt; background-color:#fff;">सर्वे कर्ता का नाम व मोबाइल नं.</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="height: 90px;"></td>
                    <td style="height: 70px;"></td>
                    <td style="height: 70px;"></td>
                </tr>
            </tbody>
        </table>
        
        <div style="text-align: center; font-size: 12pt; font-weight: bold; padding-top: 10px;">
            बुकलेट संख्या - ____________
        </div>
    </div>
</div>


<!-- ================= SAMPLE FAMILY PAGE ================= -->

<div class="family-page">

    <div style="text-align: center; font-size: 15pt; margin-bottom: 8px;">
        भारतीय सामाजिक परिवार सुरक्षा लाभार्थी चिन्हीकरण
    </div>

    <p style="text-align:center; margin-bottom:8px; font-size:16px;">
        वार्ड / ग्राम - ___________
    </p>

    <p style="font-size:13px; margin-bottom:12px;">
        परिवार संख्या - 1
        &nbsp;&nbsp;&nbsp;
        मकान नंबर - _______
        <span style="float:right;">क्षेत्र - _______</span>
    </p>

    <table style="margin-bottom:5px;">
        <tr>
            <th style="width: 6%;">क्र.स.</th>
            <th style="width: 17%;">सदस्य का नाम</th>
            <th style="width: 17%;">पिता / पति का नाम</th>
            <th style="width: 10%;">जन्मदिवस</th>
            <th style="width: 15%;">मोबाइल</th>
            <th style="width: 7%;">शिक्षा</th>
            <th style="width: 21%;">पेशा/व्यवसाय/जाति/अन्य जानकारी</th>
        </tr>

        ${Array.from({ length: 6 }, (_, i) => `
        <tr>
            <td style="text-align:center;">${i + 1}</td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
        </tr>
        `).join('')}
    </table>

    <p style="font-size:13px; margin-top: 10px;">
        ST ☐ &nbsp;&nbsp;
        SC ☐ &nbsp;&nbsp;
        OBC ☐ &nbsp;&nbsp;
        GEN ☐
    </p>

</div>

</body>
</html>
`;
};


module.exports = { generatePDFHTML, generateBlankRegisterHTML };

// 