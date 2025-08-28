// api/employees.js
const { google } = require('googleapis');

/**
 * ENV required:
 *  - GOOGLE_SHEET_ID
 *  - GOOGLE_SERVICE_ACCOUNT_EMAIL
 *  - GOOGLE_PRIVATE_KEY (with literal \n; replaced below)
 *
 * Sheet assumptions (from your file):
 *  - Tab name: "Employees"
 *  - Identity cols: "First name (legal)", "Last name (legal)", "LinkedIn profile", "Status", "Business Line"
 *  - Many follower snapshot columns with date-like labels, e.g.:
 *      "LinkedIn Following (18th March)"
 *      "4/1/2025 (15TH April)"
 *      "4/30/2025 (30TH April)"
 *      "15th June 2025"
 *      "30th July 2025"
 *      "15th August 2025"
 */

// --------------------------- Utilities ---------------------------

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const SA_PK = process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n');

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function assertEnv() {
  const missing = [];
  if (!SHEET_ID) missing.push('GOOGLE_SHEET_ID');
  if (!SA_EMAIL) missing.push('GOOGLE_SERVICE_ACCOUNT_EMAIL');
  if (!SA_PK) missing.push('GOOGLE_PRIVATE_KEY');
  if (missing.length) {
    const err = new Error(`Missing required environment variables: ${missing.join(', ')}`);
    err.code = 'ENV_VARS_MISSING';
    throw err;
  }
}

async function getSheetsClient() {
  assertEnv();
  const auth = new google.auth.JWT(
    SA_EMAIL,
    null,
    SA_PK,
    ['https://www.googleapis.com/auth/spreadsheets.readonly']
  );
  return google.sheets({ version: 'v4', auth });
}

const MONTHS = [
  'january','february','march','april','may','june',
  'july','august','september','october','november','december'
];

function removeOrdinals(str) {
  return String(str).replace(/\b(\d+)(st|nd|rd|th)\b/gi, '$1');
}

function coerceNumber(v) {
  if (v === null || typeof v === 'undefined' || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Try hard to extract a usable ISO date (yyyy-mm-dd) from a variety of header formats.
 * Strategy:
 *  1) Prefer an explicit numeric date like "4/30/2025" or "2025-07-15" anywhere in the header.
 *  2) Else parse a textual date like "18th March 2025" or "15th June" (if no year, fall back to defaultYear).
 *  3) Return null if nothing reasonable can be parsed.
 */
function parseHeaderToISODate(header, defaultYear) {
  if (!header) return null;
  const raw = removeOrdinals(header).replace(/\s+/g, ' ').trim();

  // 1) mm/dd/yyyy or m/d/yyyy
  const mdy = raw.match(/\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\b/);
  if (mdy) {
    const mm = Number(mdy[1]);
    const dd = Number(mdy[2]);
    let yyyy = Number(mdy[3]);
    if (yyyy < 100) yyyy += 2000; // just in case
    const d = new Date(Date.UTC(yyyy, mm - 1, dd));
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  // 2) yyyy-mm-dd
  const ymd = raw.match(/\b(\d{4})-(\d{1,2})-(\d{1,2})\b/);
  if (ymd) {
    const yyyy = Number(ymd[1]);
    const mm = Number(ymd[2]);
    const dd = Number(ymd[3]);
    const d = new Date(Date.UTC(yyyy, mm - 1, dd));
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  // 3) textual, like "18 March 2025" or "15 June"
  const lower = raw.toLowerCase();

  // Pull out "(...)" content if present, it's often the real date alias
  const inParens = lower.match(/\(([^)]+)\)/);
  const core = inParens ? inParens[1] : lower;

  // find month name
  const monthIndex = MONTHS.findIndex(m => core.includes(m));
  if (monthIndex >= 0) {
    // find day number near the month
    const dayMatch = core.match(/\b(\d{1,2})\b/);
    const dd = dayMatch ? Number(dayMatch[1]) : 1;
    // year?
    const yearMatch = core.match(/\b(20\d{2})\b/);
    const yyyy = yearMatch ? Number(yearMatch[1]) : (defaultYear || new Date().getUTCFullYear());

    const d = new Date(Date.UTC(yyyy, monthIndex, dd));
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  // Fallback: try to parse Date() generically (last resort)
  const d = new Date(raw);
  if (!isNaN(d)) return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString().slice(0, 10);

  return null;
}

/**
 * Determine a default year by scanning headers for any YYYY present; if none, use current year.
 */
function inferDefaultYear(headers) {
  for (const h of headers) {
    const m = String(h).match(/\b(20\d{2})\b/);
    if (m) return Number(m[1]);
  }
  return new Date().getUTCFullYear();
}

/**
 * Given a row object keyed by header strings, build:
 *  - identity fields (firstName, lastName, businessLine, linkedinProfile, status)
 *  - followers { isoDate: number }
 */
function buildEmployeeFromRow(row, dateCols) {
  const firstName = row['First name (legal)'] ?? row['First Name'] ?? row['First name'] ?? '';
  const lastName = row['Last name (legal)'] ?? row['Last Name'] ?? row['Last name'] ?? '';
  const businessLine = row['Business Line'] ?? row['Business line'] ?? row['Business'] ?? 'Unassigned';
  const status = row['Status'] ?? 'Active';
  const linkedinProfile = row['LinkedIn profile'] ?? row['Linkedin profile'] ?? row['LinkedIn'] ?? '';

  const followers = {};
  for (const { header, iso } of dateCols) {
    const n = coerceNumber(row[header]);
    if (n !== null) followers[iso] = n;
  }

  return {
    firstName: String(firstName || '').trim(),
    lastName: String(lastName || '').trim(),
    businessLine: String(businessLine || '').trim() || 'Unassigned',
    status: String(status || '').trim(),
    linkedinProfile: String(linkedinProfile || '').trim(),
    followers
  };
}

/**
 * Compute metrics from followers series:
 *  - currentFollowers = latest non-null value
 *  - earliestFollowers = earliest non-null value
 *  - absoluteGrowth = current - earliest
 *  - growthRate% = (absoluteGrowth / earliest) * 100 (0 if earliest <= 0)
 *  - consistencyScore% = % of positive-or-zero deltas across consecutive points
 */
function computeMetrics(followers) {
  const dates = Object.keys(followers).sort();
  if (dates.length === 0) {
    return {
      currentFollowers: 0,
      absoluteGrowth: 0,
      growthRate: 0,
      consistencyScore: 0
    };
  }
  const earliest = followers[dates[0]];
  const latest = followers[dates[dates.length - 1]];

  const absoluteGrowth = (latest ?? 0) - (earliest ?? 0);
  const growthRate = (earliest && earliest > 0)
    ? +( (absoluteGrowth / earliest) * 100 ).toFixed(1)
    : 0;

  // consistency: fraction of non-negative deltas
  let positives = 0;
  let steps = 0;
  for (let i = 1; i < dates.length; i++) {
    const prev = followers[dates[i - 1]];
    const curr = followers[dates[i]];
    if (prev != null && curr != null) {
      steps += 1;
      if (curr - prev >= 0) positives += 1;
    }
  }
  const consistencyScore = steps > 0 ? Math.round((positives / steps) * 100) : 0;

  return {
    currentFollowers: Number(latest ?? 0),
    absoluteGrowth: Number(absoluteGrowth),
    growthRate: Number(growthRate),
    consistencyScore: Number(consistencyScore)
  };
}

// --------------------------- Data Access ---------------------------

async function getEmployeesSheet() {
  const sheets = await getSheetsClient();

  // Read the "Employees" tab; fetch a wide range to include all date columns
  const range = 'Employees!A1:ZZ2000';
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range,
    valueRenderOption: 'UNFORMATTED_VALUE',
    dateTimeRenderOption: 'FORMATTED_STRING'
  });

  const values = resp?.data?.values || [];
  if (!values.length) return { headers: [], rows: [] };

  const [headers, ...rows] = values;
  return { headers, rows };
}

// --------------------------- Transformation ---------------------------

function detectDateColumns(headers) {
  const defaultYear = inferDefaultYear(headers);
  const dateCols = [];

  for (const header of headers) {
    // skip known identity columns
    const h = String(header || '').trim();
    const lower = h.toLowerCase();

    const isIdentity =
      lower === 'first name (legal)' ||
      lower === 'last name (legal)' ||
      lower === 'first name' ||
      lower === 'last name' ||
      lower === 'linkedin profile' ||
      lower === 'status' ||
      lower === 'business line' ||
      lower === 'business' ||
      lower === 'businessline';

    if (isIdentity) continue;

    // try to parse into a date
    const iso = parseHeaderToISODate(h, defaultYear);
    if (iso) {
      dateCols.push({ header: h, iso });
    }
  }

  // de-duplicate in case multiple headers map to the same date (keep the last)
  const byDate = new Map();
  for (const c of dateCols) byDate.set(c.iso, c);
  return Array.from(byDate.values()).sort((a, b) => a.iso.localeCompare(b.iso));
}

function rowsToObjects(headers, rows) {
  return rows.map(r => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = typeof r[i] === 'undefined' ? '' : r[i];
    }
    return obj;
  });
}

function buildEmployeesPayload(sheet) {
  const { headers, rows } = sheet;
  if (!headers.length) return { employees: [], summary: baseSummary() };

  const dateCols = detectDateColumns(headers);
  const objects = rowsToObjects(headers, rows);

  const employees = [];
  for (const row of objects) {
    // Only include Active (or empty = treat as Active) rows with at least a name
    const status = (row['Status'] ?? '').toString().trim();
    const first = (row['First name (legal)'] ?? row['First name'] ?? '').toString().trim();
    const last = (row['Last name (legal)'] ?? row['Last name'] ?? '').toString().trim();
    if (!first && !last) continue;
    if (status && status.toLowerCase() !== 'active') continue;

    const e = buildEmployeeFromRow(row, dateCols);
    e.metrics = computeMetrics(e.followers);
    employees.push(e);
  }

  // Summary
  const summary = computeSummary(employees);

  return { employees, summary };
}

function baseSummary() {
  return {
    lastUpdated: new Date().toISOString(),
    totalEmployees: 0,
    avgGrowthRate: 0,
    topGrower: null,
    totalFollowers: 0
  };
}

function computeSummary(employees) {
  const s = baseSummary();
  s.totalEmployees = employees.length;
  s.totalFollowers = employees.reduce((acc, e) => acc + (e.metrics?.currentFollowers || 0), 0);

  const withRates = employees.filter(e => typeof e.metrics?.growthRate === 'number');
  if (withRates.length) {
    const avg = withRates.reduce((acc, e) => acc + e.metrics.growthRate, 0) / withRates.length;
    s.avgGrowthRate = +avg.toFixed(1);

    const top = withRates.reduce((best, e) =>
      e.metrics.growthRate > (best?.metrics?.growthRate ?? -Infinity) ? e : best, null);
    if (top) s.topGrower = `${top.firstName || ''} ${top.lastName || ''}`.trim() || null;
  }

  return s;
}

// --------------------------- API Handler ---------------------------

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const sheet = await getEmployeesSheet();
    const payload = buildEmployeesPayload(sheet);
    return res.status(200).json(payload);
  } catch (error) {
    console.error('[employees API] Error:', error);
    return res.status(500).json({
      error: 'Failed to fetch employee data',
      code: error?.code || 'INTERNAL_ERROR',
      details: error?.message || 'Unknown error'
    });
  }
}
