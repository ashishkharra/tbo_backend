// const puppeteer = require("puppeteer");

// async function generatePDF(html) {
//   const browser = await puppeteer.launch({
//     headless: "new"
//   });

//   const page = await browser.newPage();
//   await page.setContent(html, { waitUntil: "networkidle0" });

//   const buffer = await page.pdf({
//     format: "A4",
//     printBackground: true
//   });

//   await browser.close();

//   return buffer;
// }

// module.exports = { generatePDF };


const puppeteer = require("puppeteer");

let browser;

async function getBrowser() {
  if (!browser) {
    browser = await puppeteer.launch({
      headless: "new",
      timeout: 120000,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });
    browser.on("disconnected", () => {
      browser = null;
    });
  }
  return browser;
}

async function generatePDF(html) {
  const browserInstance = await getBrowser();
  const page = await browserInstance.newPage();

  await page.setContent(html, {
    waitUntil: "domcontentloaded",
    timeout: 120000,
  });

  const pdf = await page.pdf({
    format: "A4",
    printBackground: true,
  });

  await page.close();
  return pdf;
}

module.exports = { generatePDF };
