const { google } = require('googleapis');

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
  'january', 'february', 'march', 'april', 'may', 'june',
  'july', 'august', 'september', 'october', 'november', 'december'
];

function coerceNumber(v) {
  if (v === null || typeof v === 'undefined' || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseHeaderToMonth(header, defaultYear = 2025) {
  if (!header) return null;
  const raw = String(header).trim().toLowerCase();
  
  // Direct month name matching
  const monthIndex = MONTHS.findIndex(m => raw === m);
  if (monthIndex >= 0) {
    return {
      month: MONTHS[monthIndex],
      year: defaultYear,
      sortKey: `${defaultYear}-${String(monthIndex + 1).padStart(2, '0')}`
    };
  }

  return null;
}

function buildEmployeeFromRow(row, monthCols) {
  const firstName = row['First name (legal)'] ?? row['First Name'] ?? row['First name'] ?? '';
  const lastName = row['Last name (legal)'] ?? row['Last Name'] ?? row['Last name'] ?? '';
  const businessLine = row['Business Line'] ?? row['Business line'] ?? row['Business'] ?? 'Unassigned';
  const status = row['Status'] ?? 'Active';
  const linkedinProfile = row['LinkedIn profile'] ?? row['Linkedin profile'] ?? row['LinkedIn'] ?? '';

  const monthlyFollowers = {};
  for (const { header, monthData } of monthCols) {
    const n = coerceNumber(row[header]);
    if (n !== null) {
      monthlyFollowers[monthData.sortKey] = {
        followers: n,
        month: monthData.month,
        year: monthData.year,
        displayName: `${monthData.month.charAt(0).toUpperCase() + monthData.month.slice(1)} ${monthData.year}`
      };
    }
  }

  return {
    firstName: String(firstName || '').trim(),
    lastName: String(lastName || '').trim(),
    businessLine: String(businessLine || '').trim() || 'Unassigned',
    status: String(status || '').trim(),
    linkedinProfile: String(linkedinProfile || '').trim(),
    monthlyFollowers
  };
}

function computeOverallMetrics(monthlyFollowers) {
  const sortedKeys = Object.keys(monthlyFollowers).sort();
  if (sortedKeys.length === 0) {
    return {
      currentFollowers: 0,
      absoluteGrowth: 0,
      growthRate: 0
    };
  }

  const earliest = monthlyFollowers[sortedKeys[0]];
  const latest = monthlyFollowers[sortedKeys[sortedKeys.length - 1]];

  const absoluteGrowth = latest.followers - earliest.followers;
  const growthRate = earliest.followers > 0 ? ((absoluteGrowth / earliest.followers) * 100) : 0;

  return {
    currentFollowers: latest.followers,
    absoluteGrowth: absoluteGrowth,
    growthRate: +growthRate.toFixed(1)
  };
}

function computeMonthlyMetrics(monthlyFollowers) {
  const sortedKeys = Object.keys(monthlyFollowers).sort();
  const monthlyMetrics = [];

  for (let i = 1; i < sortedKeys.length; i++) {
    const currentKey = sortedKeys[i];
    const previousKey = sortedKeys[i - 1];
    const current = monthlyFollowers[currentKey];
    const previous = monthlyFollowers[previousKey];

    const monthlyGrowth = current.followers - previous.followers;
    const monthlyGrowthRate = previous.followers > 0 ? 
      ((monthlyGrowth / previous.followers) * 100) : 0;

    monthlyMetrics.push({
      month: current.displayName,
      monthKey: currentKey,
      currentFollowers: current.followers,
      monthlyGrowth: monthlyGrowth,
      monthlyGrowthRate: +monthlyGrowthRate.toFixed(1)
    });
  }

  return monthlyMetrics;
}

function calculateMonthlyWinners(employees) {
  const monthlyWinners = [];
  const allMonths = new Set();

  // Collect all months that have data
  employees.forEach(emp => {
    if (emp.monthlyMetrics) {
      emp.monthlyMetrics.forEach(metric => {
        allMonths.add(metric.monthKey);
      });
    }
  });

  const sortedMonths = Array.from(allMonths).sort().reverse();

  sortedMonths.forEach(monthKey => {
    const monthEmployees = employees
      .map(emp => {
        if (!emp.monthlyMetrics) return null;
        const monthMetric = emp.monthlyMetrics.find(m => m.monthKey === monthKey);
        if (!monthMetric || monthMetric.monthlyGrowth <= 0) return null;
        return {
          ...emp,
          monthMetric: monthMetric
        };
      })
      .filter(emp => emp !== null)
      .sort((a, b) => b.monthMetric.monthlyGrowth - a.monthMetric.monthlyGrowth);

    if (monthEmployees.length > 0) {
      const winner = monthEmployees[0];
      monthlyWinners.push({
        month: winner.monthMetric.month,
        monthKey: monthKey,
        winner: {
          name: `${winner.firstName || ''} ${winner.lastName || ''}`.trim(),
          businessLine: winner.businessLine,
          currentFollowers: winner.monthMetric.currentFollowers,
          monthlyGrowth: winner.monthMetric.monthlyGrowth,
          monthlyGrowthRate: winner.monthMetric.monthlyGrowthRate,
          linkedinProfile: winner.linkedinProfile
        }
      });
    }
  });

  return monthlyWinners;
}

// --------------------------- Data Access ---------------------------

async function getEmployeesSheet() {
  const sheets = await getSheetsClient();
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

function detectMonthColumns(headers) {
  const monthCols = [];

  for (const header of headers) {
    const h = String(header || '').trim();
    const lower = h.toLowerCase();

    // Skip identity columns
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

    const monthData = parseHeaderToMonth(h);
    if (monthData) {
      monthCols.push({ header: h, monthData });
    }
  }

  return monthCols.sort((a, b) => a.monthData.sortKey.localeCompare(b.monthData.sortKey));
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
  if (!headers.length) return { employees: [], monthlyWinners: [], summary: baseSummary() };

  const monthCols = detectMonthColumns(headers);
  const objects = rowsToObjects(headers, rows);

  const employees = [];
  for (const row of objects) {
    const status = (row['Status'] ?? '').toString().trim();
    const first = (row['First name (legal)'] ?? row['First name'] ?? '').toString().trim();
    const last = (row['Last name (legal)'] ?? row['Last name'] ?? '').toString().trim();
    if (!first && !last) continue;
    if (status && status.toLowerCase() !== 'active') continue;

    const e = buildEmployeeFromRow(row, monthCols);
    
    // Skip employees with no month data
    if (Object.keys(e.monthlyFollowers).length === 0) continue;
    
    e.metrics = computeOverallMetrics(e.monthlyFollowers);
    e.monthlyMetrics = computeMonthlyMetrics(e.monthlyFollowers);
    employees.push(e);
  }

  const summary = computeSummary(employees);
  const monthlyWinners = calculateMonthlyWinners(employees);

  return { employees, monthlyWinners, summary };
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
};
