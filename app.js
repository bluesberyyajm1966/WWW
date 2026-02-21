'use strict';

const S = {
  countries:   [],
  filtered:    [],
  selected:    null,
  region:      'all',
  query:       '',
  countryData: {},  // World Bank — keyed by ISO3
  imfData:     {},  // IMF       — keyed by ISO3 or ISO2
  oecdData:    {},  // OECD      — keyed by ISO2
  unData:      {},  // UN        — keyed by ISO3
  owidData:    {},  // OWID      — keyed by ISO3
  unescoData:  {},  // UNESCO    — keyed by ISO3
  sipriData:   {},  // SIPRI     — keyed by ISO3
};

// ── UTILS ────────────────────────────────────────────

function flag(iso2) {
  if (!iso2 || iso2.length !== 2) return '🌐';
  return [...iso2.toUpperCase()]
    .map(c => String.fromCodePoint(0x1F1E6 + c.charCodeAt(0) - 65))
    .join('');
}

function fmt(val, decimals = 1) {
  if (val == null || val === '' || isNaN(+val)) return null;
  const n = parseFloat(val);
  if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(decimals) + 'T';
  if (Math.abs(n) >= 1e9)  return (n / 1e9).toFixed(decimals)  + 'B';
  if (Math.abs(n) >= 1e6)  return (n / 1e6).toFixed(decimals)  + 'M';
  if (Math.abs(n) >= 1e3)  return n.toLocaleString(undefined, { maximumFractionDigits: decimals });
  return n.toFixed(decimals);
}

function fmtPct(val, decimals = 1) {
  if (val == null || val === '' || isNaN(+val)) return null;
  return parseFloat(val).toFixed(decimals) + '%';
}

function fmtUSD(val, decimals = 2) {
  const f = fmt(val, decimals);
  return f ? '$' + f : null;
}

function sfx(val, decimals, unit) {
  const f = fmt(val, decimals);
  return f ? f + unit : null;
}

function wb(iso3, field)   { return S.countryData[iso3]?.[field] ?? null; }
function wbyr(iso3, field) { return S.countryData[iso3]?.[field + '_year'] ?? null; }

function imf(c, field) {
  const d = S.imfData[c.id] || S.imfData[c.iso2] || null;
  return d?.[field] ?? null;
}
function imfyr(c, field) {
  const d = S.imfData[c.id] || S.imfData[c.iso2] || null;
  return d?.[field + '_year'] ?? null;
}

function oecd(c, field) {
  const d = S.oecdData[c.iso2] || S.oecdData[c.id] || null;
  return d?.[field] ?? null;
}
function oecdyr(c, field) {
  const d = S.oecdData[c.iso2] || S.oecdData[c.id] || null;
  return d?.[field + '_year'] ?? null;
}

function un(c, field)   { return S.unData[c.id]?.[field] ?? null; }
function unyr(c, field) { return S.unData[c.id]?.[field + '_year'] ?? null; }

function owid(c, field)   { return S.owidData[c.id]?.[field] ?? null; }
function owidyr(c, field) { return S.owidData[c.id]?.[field + '_year'] ?? null; }

function unesco(c, field)   { return S.unescoData[c.id]?.[field] ?? null; }
function unescoyr(c, field) { return S.unescoData[c.id]?.[field + '_year'] ?? null; }

function sipri(c, field)   { return S.sipriData[c.id]?.[field] ?? null; }
function sipriyr(c, field) { return S.sipriData[c.id]?.[field + '_year'] ?? null; }

function toast(msg, duration = 4000) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove('show'), duration);
}

async function fetchJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${r.status} ${path}`);
  return r.json();
}

// ── LOAD DATA ────────────────────────────────────────

async function loadAllData() {
  try {
    const json = await fetchJSON('data/worldbank/worldbank_data.json');
    const raw = Array.isArray(json) ? json : (json.countries || []);
    S.countries = parseWorldBank(raw);
    console.log(`✓ worldbank_data.json — ${S.countries.length} countries`);
  } catch (e) {
    console.error('worldbank_data.json missing:', e.message);
    toast('⚠ worldbank_data.json not found', 8000);
    document.getElementById('countLabel').textContent = '⚠ Missing file';
    return;
  }

  try {
    S.countryData = await fetchJSON('data/worldbank/country_data.json');
    console.log(`✓ country_data.json — ${Object.keys(S.countryData).length} entries`);
  } catch { console.warn('country_data.json not found'); }

  try {
    S.imfData = await fetchJSON('data/imf/imf_data.json');
    console.log(`✓ imf_data.json — ${Object.keys(S.imfData).length} entries`);
  } catch { console.warn('imf_data.json not found'); }

  try {
    S.oecdData = await fetchJSON('data/oecd/oecd_data.json');
    console.log(`✓ oecd_data.json — ${Object.keys(S.oecdData).length} entries`);
  } catch { console.warn('oecd_data.json not found'); }

  try {
    S.owidData = await fetchJSON('data/owid/owid_data.json');
    console.log(`✓ owid_data.json — ${Object.keys(S.owidData).length} entries`);
  } catch { console.warn('owid_data.json not found — run fetch_owid.py'); }

  try {
    S.unescoData = await fetchJSON('data/unesco/unesco_data.json');
    console.log(`✓ unesco_data.json — ${Object.keys(S.unescoData).length} entries`);
  } catch { console.warn('unesco_data.json not found — run fetch_unesco.py'); }

  try {
    S.sipriData = await fetchJSON('data/sipri/sipri_data.json');
    console.log(`✓ sipri_data.json — ${Object.keys(S.sipriData).length} entries`);
  } catch { console.warn('sipri_data.json not found — run fetch_sipri.py'); }

  try {
    S.unData = await fetchJSON('data/un/un_data.json');
    console.log(`✓ un_data.json — ${Object.keys(S.unData).length} entries`);
  } catch { console.warn('un_data.json not found — run fetch_un.py'); }

  const sources = [
    Object.keys(S.countryData).length > 0 ? 'World Bank' : null,
    Object.keys(S.imfData).length > 0     ? 'IMF'        : null,
    Object.keys(S.oecdData).length > 0    ? 'OECD'       : null,
    Object.keys(S.unData).length > 0      ? 'UN'         : null,
  ].filter(Boolean);
  toast(`✓ Loaded: ${sources.join(' · ')}`);

  buildFilters();
  applyFilters();
}

function parseWorldBank(raw) {
  return raw
    .filter(c => c.capitalCity && c.capitalCity.trim())
    .map(c => ({
      id:          c.id,
      iso2:        c.iso2Code,
      name:        c.name,
      capital:     c.capitalCity,
      region:      c.region?.value?.trim()      || '',
      regionId:    c.region?.id                 || '',
      adminRegion: c.adminregion?.value?.trim() || '',
      income:      c.incomeLevel?.value         || '',
      incomeId:    c.incomeLevel?.id            || '',
      lending:     c.lendingType?.value         || '',
      lendingId:   c.lendingType?.id            || '',
      lon:         c.longitude                  || '',
      lat:         c.latitude                   || '',
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

// ── FILTERS ──────────────────────────────────────────

const REGION_SHORT = {
  'East Asia & Pacific':                               'E. Asia',
  'Europe & Central Asia':                             'Europe',
  'Latin America & Caribbean':                         'LatAm',
  'Middle East & North Africa':                        'MENA',
  'Middle East, North Africa, Afghanistan & Pakistan': 'MENA',
  'North America':                                     'N. America',
  'South Asia':                                        'S. Asia',
  'Sub-Saharan Africa':                                'Africa',
};

function buildFilters() {
  const regions = [...new Set(S.countries.map(c => c.region).filter(Boolean))].sort();
  const row = document.getElementById('filterRow');
  row.innerHTML = '<button class="filter-btn active" data-region="all">All</button>';
  regions.forEach(r => {
    const b = document.createElement('button');
    b.className = 'filter-btn';
    b.dataset.region = r;
    b.textContent = REGION_SHORT[r] || r.split(' ')[0];
    b.title = r;
    row.appendChild(b);
  });
  row.addEventListener('click', e => {
    const btn = e.target.closest('.filter-btn');
    if (!btn) return;
    row.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    S.region = btn.dataset.region;
    applyFilters();
  });
}

document.getElementById('searchInput').addEventListener('input', e => {
  S.query = e.target.value;
  applyFilters();
});

function applyFilters() {
  const q = S.query.toLowerCase();
  S.filtered = S.countries.filter(c => {
    const rOK = S.region === 'all' || c.region === S.region;
    const sOK = !q ||
      c.name.toLowerCase().includes(q)         ||
      c.capital.toLowerCase().includes(q)      ||
      (c.iso2 || '').toLowerCase().includes(q) ||
      c.id.toLowerCase().includes(q);
    return rOK && sOK;
  });
  renderList();
  document.getElementById('countLabel').textContent =
    `${S.filtered.length} ${S.filtered.length === 1 ? 'country' : 'countries'}`;
}

// ── SIDEBAR LIST ─────────────────────────────────────

function renderList() {
  const list = document.getElementById('countryList');
  if (S.filtered.length === 0) {
    list.innerHTML = '<div class="no-results">No countries found</div>';
    return;
  }
  list.innerHTML = '';
  const frag = document.createDocumentFragment();
  S.filtered.forEach((c, i) => {
    const div = document.createElement('div');
    div.className = 'country-item' + (S.selected === c.iso2 ? ' active' : '');
    div.style.animationDelay = `${Math.min(i * 5, 150)}ms`;
    div.dataset.iso2 = c.iso2;
    div.innerHTML = `
      <div class="flag">${flag(c.iso2)}</div>
      <div class="country-info">
        <div class="country-name">${c.name}</div>
        <div class="country-region">${c.region}</div>
      </div>`;
    div.addEventListener('click', () => selectCountry(c));
    frag.appendChild(div);
  });
  list.appendChild(frag);
}

// ── PROFILE ──────────────────────────────────────────

function selectCountry(c) {
  S.selected = c.iso2;
  document.querySelectorAll('.country-item').forEach(el =>
    el.classList.toggle('active', el.dataset.iso2 === c.iso2)
  );
  renderProfile(c);
}

function renderProfile(c) {
  const id = c.id;
  document.getElementById('main').innerHTML = `
    <div class="profile">
      <div class="profile-hero">
        <div class="profile-flag">${flag(c.iso2)}</div>
        <div class="profile-titles">
          <div class="profile-name">${c.name}</div>
          <div class="profile-subtitle">
            <span>🏛 ${c.capital}</span>
            <span>🌍 ${c.region}</span>
            ${c.lat ? `<span>📍 ${parseFloat(c.lat).toFixed(2)}°, ${parseFloat(c.lon).toFixed(2)}°</span>` : ''}
          </div>
          <div class="profile-tags">
            <span class="tag">${c.income || '—'}</span>
            ${c.lending && c.lending !== 'Not classified' ? `<span class="tag">${c.lending}</span>` : ''}
            ${c.adminRegion ? `<span class="tag accent">${c.adminRegion}</span>` : ''}
          </div>
        </div>
      </div>
      ${renderClassification(c)}
      ${renderIMFEconomy(c)}
      ${renderWBEconomy(id)}
      ${renderOECDLabour(c)}
      ${renderOECDFiscal(c)}
      ${renderOECDInnovation(c)}
      ${renderOECDHealth(c)}
      ${renderOECDEducation(c)}
      ${renderOECDEnvironment(c)}
      ${renderOECDInequality(c)}
      ${renderUNHumanDev(c)}
      ${renderUNPopulation(c)}
      ${renderUNHealth(c)}
      ${renderDemographics(id)}
      ${renderHealth(id)}
      ${renderEducation(id)}
      ${renderEnvironment(id)}
      ${renderPoverty(id)}
      ${renderOWIDEnergy(c)}
      ${renderOWIDClimate(c)}
      ${renderOWIDHealth(c)}
      ${renderOWIDSociety(c)}
      ${renderOWIDFood(c)}
      ${renderUNESCOEducation(c)}
      ${renderUNESCOScience(c)}
      ${renderSIPRI(c)}
    </div>`;
}

// ── SECTION RENDERERS ────────────────────────────────

function renderClassification(c) {
  return `
    <p class="section-label">Classification <span class="src-badge src-wb">World Bank</span></p>
    <div class="stats-grid">
      ${statCard('ISO2 Code',    c.iso2,    '2-letter code')}
      ${statCard('ISO3 Code',    c.id,      '3-letter code')}
      ${statCard('Region',       c.region,  'WB grouping',       true)}
      ${statCard('Income Level', c.income,  `ID: ${c.incomeId}`, true)}
      ${statCard('Lending Type', c.lending, `ID: ${c.lendingId}`, true)}
      ${c.adminRegion ? statCard('Admin Region', c.adminRegion, '', true) : ''}
    </div>`;
}

function renderIMFEconomy(c) {
  const hasIMFFile = Object.keys(S.imfData).length > 0;
  const rows = [
    dataRow('Real GDP Growth',          fmtPct(imf(c,'gdpGrowth')),         imfyr(c,'gdpGrowth')),
    dataRow('GDP (current USD)',         sfx(imf(c,'gdpUSD'), 2, 'B'),       imfyr(c,'gdpUSD')),
    dataRow('GDP per Capita (USD)',      fmtUSD(imf(c,'gdpPerCapUSD')),      imfyr(c,'gdpPerCapUSD')),
    dataRow('GDP (PPP, intl $)',         sfx(imf(c,'gdpPPP'), 2, 'B'),       imfyr(c,'gdpPPP')),
    dataRow('GDP per Capita (PPP)',      fmtUSD(imf(c,'gdpPerCapPPP')),      imfyr(c,'gdpPerCapPPP')),
    dataRow('Share of World GDP (PPP)', fmtPct(imf(c,'gdpShareWorld'), 2),  imfyr(c,'gdpShareWorld')),
    dataRow('Inflation (CPI)',           fmtPct(imf(c,'inflation')),         imfyr(c,'inflation')),
    dataRow('Unemployment Rate',         fmtPct(imf(c,'unemployment')),      imfyr(c,'unemployment')),
    dataRow('Employment',               sfx(imf(c,'employment'), 1, 'M'),   imfyr(c,'employment')),
    dataRow('Fiscal Balance (% GDP)',   fmtPct(imf(c,'fiscalBalance')),     imfyr(c,'fiscalBalance')),
    dataRow('Govt Debt (% GDP)',        fmtPct(imf(c,'govtDebt')),          imfyr(c,'govtDebt')),
    dataRow('Govt Revenue (% GDP)',     fmtPct(imf(c,'govtRevenue')),       imfyr(c,'govtRevenue')),
    dataRow('Govt Expenditure (% GDP)', fmtPct(imf(c,'govtExpenditure')),   imfyr(c,'govtExpenditure')),
    dataRow('Current Account (% GDP)',  fmtPct(imf(c,'currentAccount')),    imfyr(c,'currentAccount')),
    dataRow('Export Volume Growth',     fmtPct(imf(c,'exportGrowth')),      imfyr(c,'exportGrowth')),
    dataRow('Import Volume Growth',     fmtPct(imf(c,'importGrowth')),      imfyr(c,'importGrowth')),
    dataRow('Total Investment (% GDP)', fmtPct(imf(c,'investment')),        imfyr(c,'investment')),
    dataRow('Gross Savings (% GDP)',    fmtPct(imf(c,'savings')),           imfyr(c,'savings')),
  ].filter(Boolean);

  const noDataMsg = hasIMFFile
    ? `<div class="no-data-row">No IMF data available for this country</div>`
    : `<div class="no-data-row">IMF data not loaded — run <code>python3 fetch_imf_data.py</code> from scripts/</div>`;

  return `
    <p class="section-label">Economy — IMF <span class="src-badge src-imf">IMF</span></p>
    <div class="data-section">
      <div class="data-section-header"><h3>IMF Economic Indicators</h3></div>
      ${rows.join('') || noDataMsg}
    </div>`;
}

function renderWBEconomy(id) {
  return section('Economy — World Bank', 'wb', 'World Bank Economic Indicators', [
    dataRow('GDP (Total)',          fmtUSD(wb(id,'gdp')),           wbyr(id,'gdp')),
    dataRow('GDP per Capita',       fmtUSD(wb(id,'gdpPerCap')),     wbyr(id,'gdpPerCap')),
    dataRow('GDP per Capita (PPP)', fmtUSD(wb(id,'gdpPerCapPPP')), wbyr(id,'gdpPerCapPPP')),
    dataRow('GNI per Capita',       fmtUSD(wb(id,'gniPerCap')),     wbyr(id,'gniPerCap')),
    dataRow('GDP Growth',           fmtPct(wb(id,'gdpGrowth')),     wbyr(id,'gdpGrowth')),
    dataRow('Inflation Rate',       fmtPct(wb(id,'inflation')),     wbyr(id,'inflation')),
    dataRow('Unemployment Rate',    fmtPct(wb(id,'unemployment')),  wbyr(id,'unemployment')),
    dataRow('Trade (% of GDP)',     fmtPct(wb(id,'tradeGDP')),      wbyr(id,'tradeGDP')),
    dataRow('FDI Net Inflows',      fmtUSD(wb(id,'fdi')),           wbyr(id,'fdi')),
    dataRow('Gini Index',           fmt(wb(id,'gini'), 1),          wbyr(id,'gini')),
  ]);
}

// ── OECD SECTIONS ────────────────────────────────────

function renderOECDLabour(c) {
  return section('Labour Market — OECD', 'oecd', 'OECD Labour Indicators', [
    dataRow('Avg Hours Worked per Year', fmt(oecd(c,'hoursWorked'), 0),       oecdyr(c,'hoursWorked')),
    dataRow('Trade Union Density',       fmtPct(oecd(c,'unionDensity')),      oecdyr(c,'unionDensity')),
    dataRow('Real Wage Growth',          fmtPct(oecd(c,'wageGrowth')),        oecdyr(c,'wageGrowth')),
    dataRow('GDP per Hour Worked',       fmtUSD(oecd(c,'gdpPerHour')),        oecdyr(c,'gdpPerHour')),
  ]);
}

function renderOECDFiscal(c) {
  return section('Tax & Fiscal — OECD', 'oecd', 'OECD Tax & Fiscal Indicators', [
    dataRow('Tax Revenue (% GDP)',      fmtPct(oecd(c,'taxRevenue')),      oecdyr(c,'taxRevenue')),
    dataRow('Social Spending (% GDP)',  fmtPct(oecd(c,'socialSpending')),  oecdyr(c,'socialSpending')),
    dataRow('Pension Spending (% GDP)', fmtPct(oecd(c,'pensionSpend')),    oecdyr(c,'pensionSpend')),
    dataRow('Family Spending (% GDP)',  fmtPct(oecd(c,'familySpend')),     oecdyr(c,'familySpend')),
    dataRow('Unemployment Benefits',    fmtPct(oecd(c,'unempBenefits')),   oecdyr(c,'unempBenefits')),
  ]);
}

function renderOECDInnovation(c) {
  return section('Innovation — OECD', 'oecd', 'OECD Innovation & Productivity', [
    dataRow('R&D Expenditure (% GDP)', fmtPct(oecd(c,'rdSpending')),  oecdyr(c,'rdSpending')),
    dataRow('GDP per Hour Worked',     fmtUSD(oecd(c,'gdpPerHour')), oecdyr(c,'gdpPerHour')),
  ]);
}

function renderOECDHealth(c) {
  return section('Health — OECD', 'oecd', 'OECD Health Indicators', [
    dataRow('Hospital Beds per 1,000', fmt(oecd(c,'hospitalBeds'), 1), oecdyr(c,'hospitalBeds')),
    dataRow('Nurses per 1,000',        fmt(oecd(c,'nurses'), 1),       oecdyr(c,'nurses')),
    dataRow('Doctors per 1,000',       fmt(oecd(c,'doctors'), 1),      oecdyr(c,'doctors')),
  ]);
}

function renderOECDEducation(c) {
  return section('Education — OECD', 'oecd', 'OECD Education Indicators', [
    dataRow('PISA Math Score',    fmt(oecd(c,'pisaMath'), 0),    oecdyr(c,'pisaMath')),
    dataRow('PISA Reading Score', fmt(oecd(c,'pisaRead'), 0),    oecdyr(c,'pisaRead')),
    dataRow('PISA Science Score', fmt(oecd(c,'pisaScience'), 0), oecdyr(c,'pisaScience')),
  ]);
}

function renderOECDEnvironment(c) {
  return section('Environment — OECD', 'oecd', 'OECD Environment Indicators', [
    dataRow('GHG Emissions (Mt CO2 eq)', fmt(oecd(c,'ghgEmissions'), 1),   oecdyr(c,'ghgEmissions')),
    dataRow('Municipal Waste (kg/cap)',  fmt(oecd(c,'municipalWaste'), 0), oecdyr(c,'municipalWaste')),
  ]);
}

function renderOECDInequality(c) {
  return section('Inequality — OECD', 'oecd', 'OECD Inequality & Income', [
    dataRow('Gini Coefficient',        fmt(oecd(c,'gini'), 2),        oecdyr(c,'gini')),
    dataRow('Poverty Rate',            fmtPct(oecd(c,'povertyRate')), oecdyr(c,'povertyRate')),
    dataRow('Median Household Income', fmtUSD(oecd(c,'medianIncome')),oecdyr(c,'medianIncome')),
    dataRow('Palma Ratio',             fmt(oecd(c,'palmaRatio'), 2),  oecdyr(c,'palmaRatio')),
  ]);
}

// ── UN SECTIONS ──────────────────────────────────────

function renderUNHumanDev(c) {
  return section('Human Development — UN', 'un', 'UN Human Development Report', [
    dataRow('Human Development Index',        fmt(un(c,'hdi'), 3),              unyr(c,'hdi')),
    dataRow('HDI Rank',                       fmt(un(c,'hdiRank'), 0),           unyr(c,'hdiRank')),
    dataRow('Inequality-adj HDI (IHDI)',      fmt(un(c,'hdiAdjInequality'), 3),  unyr(c,'hdiAdjInequality')),
    dataRow('Gender Inequality Index (GII)',  fmt(un(c,'gii'), 3),               unyr(c,'gii')),
    dataRow('GII Rank',                       fmt(un(c,'giiRank'), 0),            unyr(c,'giiRank')),
    dataRow('Gender Development Index (GDI)', fmt(un(c,'gdi'), 3),              unyr(c,'gdi')),
    dataRow('Mean Years of Schooling',        sfx(un(c,'meanSchooling'), 1, ' yrs'), unyr(c,'meanSchooling')),
    dataRow('Expected Years of Schooling',    sfx(un(c,'expSchooling'), 1, ' yrs'),  unyr(c,'expSchooling')),
    dataRow('GNI per Capita (PPP)',           fmtUSD(un(c,'gniPerCap')),         unyr(c,'gniPerCap')),
    dataRow('Income Index',                   fmt(un(c,'incomeIndex'), 3),        unyr(c,'incomeIndex')),
    dataRow('Education Index',                fmt(un(c,'educationIndex'), 3),     unyr(c,'educationIndex')),
    dataRow('Health Index',                   fmt(un(c,'healthIndex'), 3),        unyr(c,'healthIndex')),
    dataRow('Multidimensional Poverty Index', fmt(un(c,'multidimPoverty'), 3),    unyr(c,'multidimPoverty')),
    dataRow('Vulnerable Employment (%)',      fmtPct(un(c,'vulnerableEmploy')),   unyr(c,'vulnerableEmploy')),
    dataRow('CO2 Emissions per Capita',       sfx(un(c,'co2PerCap'), 2, ' t'),   unyr(c,'co2PerCap')),
    dataRow('Adolescent Birth Rate',          sfx(un(c,'adolBirthRate'), 1, ' per 1,000'), unyr(c,'adolBirthRate')),
    dataRow('Remittances (% GNI)',            fmtPct(un(c,'remittances')),        unyr(c,'remittances')),
    dataRow('Foreign Aid Received (% GNI)',   fmtPct(un(c,'foreignAid')),         unyr(c,'foreignAid')),
  ]);
}

function renderUNPopulation(c) {
  return section('Population — UN', 'un', 'UN Population Division', [
    dataRow('Total Population',        sfx(un(c,'popTotal'), 0, 'K'),            unyr(c,'popTotal')),
    dataRow('Male Population',         sfx(un(c,'popMale'), 0, 'K'),             unyr(c,'popMale')),
    dataRow('Female Population',       sfx(un(c,'popFemale'), 0, 'K'),           unyr(c,'popFemale')),
    dataRow('Population Density',      sfx(un(c,'popDensity'), 1, ' /km²'),      unyr(c,'popDensity')),
    dataRow('Population Growth Rate',  fmtPct(un(c,'popGrowthRate')),            unyr(c,'popGrowthRate')),
    dataRow('Median Age',              sfx(un(c,'medianAge'), 1, ' yrs'),         unyr(c,'medianAge')),
    dataRow('Fertility Rate',          sfx(un(c,'fertilityRate'), 2, ' births/woman'), unyr(c,'fertilityRate')),
    dataRow('Birth Rate',              sfx(un(c,'birthRate'), 1, ' per 1,000'),  unyr(c,'birthRate')),
    dataRow('Death Rate',              sfx(un(c,'deathRate'), 1, ' per 1,000'),  unyr(c,'deathRate')),
    dataRow('Net Migration Rate',      sfx(un(c,'netMigration'), 1, ' per 1,000'), unyr(c,'netMigration')),
    dataRow('Urban Population (%)',    fmtPct(un(c,'urbanPct')),                  unyr(c,'urbanPct')),
    dataRow('Population aged 0-14',   fmtPct(un(c,'popAge0to14')),               unyr(c,'popAge0to14')),
    dataRow('Population aged 65+',    fmtPct(un(c,'popAge65plus')),              unyr(c,'popAge65plus')),
    dataRow('Dependency Ratio',        fmt(un(c,'dependencyRatio'), 1),           unyr(c,'dependencyRatio')),
    dataRow('Contraceptive Use',       fmtPct(un(c,'contraceptiveUse')),          unyr(c,'contraceptiveUse')),
  ]);
}

function renderUNHealth(c) {
  return section('Health — UN', 'un', 'UN Health & Mortality', [
    dataRow('Life Expectancy (both)',   sfx(un(c,'lifeExpBoth'), 1, ' yrs'),      unyr(c,'lifeExpBoth')),
    dataRow('Life Expectancy (male)',   sfx(un(c,'lifeExpMale'), 1, ' yrs'),      unyr(c,'lifeExpMale')),
    dataRow('Life Expectancy (female)', sfx(un(c,'lifeExpFemale'), 1, ' yrs'),   unyr(c,'lifeExpFemale')),
    dataRow('Infant Mortality',         sfx(un(c,'infantMortality'), 1, ' per 1,000'), unyr(c,'infantMortality')),
    dataRow('Under-5 Mortality',        sfx(un(c,'under5Mortality'), 1, ' per 1,000'), unyr(c,'under5Mortality')),
    dataRow('Neonatal Mortality',       sfx(un(c,'neonateMortality'), 1, ' per 1,000'), unyr(c,'neonateMortality')),
    dataRow('Maternal Mortality',       sfx(un(c,'maternMortality'), 0, ' per 100,000'), unyr(c,'maternMortality')),
  ]);
}

// ── WORLD BANK SECTIONS ──────────────────────────────

function renderDemographics(id) {
  return section('Demographics', 'wb', 'World Bank Demographics', [
    dataRow('Population',         fmt(wb(id,'population'), 0),                     wbyr(id,'population')),
    dataRow('Population Growth',  fmtPct(wb(id,'popGrowth')),                      wbyr(id,'popGrowth')),
    dataRow('Population Density', sfx(wb(id,'popDensity'), 1, ' /km²'),            wbyr(id,'popDensity')),
    dataRow('Urban Population',   fmtPct(wb(id,'urbanPct')),                       wbyr(id,'urbanPct')),
    dataRow('Fertility Rate',     sfx(wb(id,'fertilityRate'), 2, ' births/woman'), wbyr(id,'fertilityRate')),
  ]);
}

function renderHealth(id) {
  return section('Health', 'wb', 'World Bank Health Indicators', [
    dataRow('Life Expectancy',       sfx(wb(id,'lifeExp'), 1, ' yrs'),                   wbyr(id,'lifeExp')),
    dataRow('Life Expectancy (F)',   sfx(wb(id,'lifeExpFemale'), 1, ' yrs'),             wbyr(id,'lifeExpFemale')),
    dataRow('Life Expectancy (M)',   sfx(wb(id,'lifeExpMale'), 1, ' yrs'),               wbyr(id,'lifeExpMale')),
    dataRow('Infant Mortality',      sfx(wb(id,'infantMortality'), 1, ' per 1,000'),     wbyr(id,'infantMortality')),
    dataRow('Maternal Mortality',    sfx(wb(id,'maternalMortality'), 0, ' per 100,000'), wbyr(id,'maternalMortality')),
    dataRow('Health Spend (% GDP)',  fmtPct(wb(id,'healthSpendGDP')),                   wbyr(id,'healthSpendGDP')),
    dataRow('Physicians per 1,000', fmt(wb(id,'physicians'), 2),                        wbyr(id,'physicians')),
    dataRow('Undernourishment',      fmtPct(wb(id,'undernourishment')),                 wbyr(id,'undernourishment')),
  ]);
}

function renderEducation(id) {
  return section('Education', 'wb', 'World Bank Education Indicators', [
    dataRow('Literacy Rate (Adult)',   fmtPct(wb(id,'literacyRate')),        wbyr(id,'literacyRate')),
    dataRow('Primary Enrollment',      fmtPct(wb(id,'primaryEnrollment')),   wbyr(id,'primaryEnrollment')),
    dataRow('Secondary Enrollment',    fmtPct(wb(id,'secondaryEnrollment')), wbyr(id,'secondaryEnrollment')),
    dataRow('Tertiary Enrollment',     fmtPct(wb(id,'tertiaryEnrollment')),  wbyr(id,'tertiaryEnrollment')),
    dataRow('Education Spend (% GDP)', fmtPct(wb(id,'educationSpendGDP')),  wbyr(id,'educationSpendGDP')),
  ]);
}

function renderEnvironment(id) {
  return section('Environment', 'wb', 'World Bank Environment & Energy', [
    dataRow('Forest Area',           fmtPct(wb(id,'forestArea')),                      wbyr(id,'forestArea')),
    dataRow('Renewable Energy',      fmtPct(wb(id,'renewableEnergy')),                 wbyr(id,'renewableEnergy')),
    dataRow('Access to Electricity', fmtPct(wb(id,'accessElectricity')),               wbyr(id,'accessElectricity')),
    dataRow('Freshwater per Capita', sfx(wb(id,'freshwaterPerCap'), 0, ' m³/yr'),      wbyr(id,'freshwaterPerCap')),
    dataRow('Internet Users',        fmtPct(wb(id,'internetUsers')),                   wbyr(id,'internetUsers')),
    dataRow('Mobile Subscriptions',  sfx(wb(id,'mobileSubscriptions'), 1, ' per 100'), wbyr(id,'mobileSubscriptions')),
  ]);
}

function renderPoverty(id) {
  return section('Poverty & Inequality', 'wb', 'World Bank Poverty & Inequality', [
    dataRow('Poverty Rate ($2.15/day)', fmtPct(wb(id,'povertyRate')),     wbyr(id,'povertyRate')),
    dataRow('Poverty Rate ($5.50/day)', fmtPct(wb(id,'povertyRatio550')), wbyr(id,'povertyRatio550')),
    dataRow('Gini Index',               fmt(wb(id,'gini'), 1),            wbyr(id,'gini')),
  ]);
}


// ── OWID SECTIONS ────────────────────────────────────

function renderOWIDEnergy(c) {
  return section('Electricity Mix — OWID', 'owid', 'Our World in Data — Electricity Generation', [
    dataRow('Solar Generation',      sfx(owid(c,'solarElec'), 1, ' TWh'),       owidyr(c,'solarElec')),
    dataRow('Wind Generation',       sfx(owid(c,'windElec'), 1, ' TWh'),        owidyr(c,'windElec')),
    dataRow('Nuclear Generation',    sfx(owid(c,'nuclearElec'), 1, ' TWh'),     owidyr(c,'nuclearElec')),
    dataRow('Fossil Fuel Generation',sfx(owid(c,'fossilElec'), 1, ' TWh'),      owidyr(c,'fossilElec')),
    dataRow('Renewables Generation', sfx(owid(c,'renewableElec'), 1, ' TWh'),   owidyr(c,'renewableElec')),
    dataRow('Hydro Generation',      sfx(owid(c,'hydroElec'), 1, ' TWh'),       owidyr(c,'hydroElec')),
    dataRow('Solar Share',           fmtPct(owid(c,'solarShare')),              owidyr(c,'solarShare')),
    dataRow('Wind Share',            fmtPct(owid(c,'windShare')),               owidyr(c,'windShare')),
    dataRow('Nuclear Share',         fmtPct(owid(c,'nuclearShare')),            owidyr(c,'nuclearShare')),
    dataRow('Fossil Share',          fmtPct(owid(c,'fossilShareElec')),         owidyr(c,'fossilShareElec')),
    dataRow('Renewables Share',      fmtPct(owid(c,'renewableShare')),          owidyr(c,'renewableShare')),
  ]);
}

function renderOWIDClimate(c) {
  return section('Climate & Emissions — OWID', 'owid', 'Our World in Data — CO₂ & Greenhouse Gases', [
    dataRow('Annual CO₂ (Mt)',          sfx(owid(c,'co2'), 2, ' Mt'),               owidyr(c,'co2')),
    dataRow('CO₂ per Capita',           sfx(owid(c,'co2PerCap'), 2, ' t'),          owidyr(c,'co2PerCap')),
    dataRow('CO₂ per GDP (kg/$)',       sfx(owid(c,'co2PerGDP'), 3, ' kg/$'),       owidyr(c,'co2PerGDP')),
    dataRow('Total GHG (Mt CO₂eq)',     sfx(owid(c,'totalGhg'), 1, ' Mt'),          owidyr(c,'totalGhg')),
    dataRow('Methane (Mt CO₂eq)',       sfx(owid(c,'methane'), 1, ' Mt'),           owidyr(c,'methane')),
    dataRow('Nitrous Oxide (Mt CO₂eq)',sfx(owid(c,'nitrousOxide'), 1, ' Mt'),      owidyr(c,'nitrousOxide')),
    dataRow('Share of Global CO₂',      fmtPct(owid(c,'shareGlobalCo2'), 2),        owidyr(c,'shareGlobalCo2')),
    dataRow('Cumulative CO₂ (Mt)',      sfx(owid(c,'cumulativeCo2'), 0, ' Mt'),     owidyr(c,'cumulativeCo2')),
    dataRow('Coal CO₂ (Mt)',            sfx(owid(c,'coalCo2'), 1, ' Mt'),           owidyr(c,'coalCo2')),
  ]);
}

function renderOWIDHealth(c) {
  return section('Health & Mortality — OWID', 'owid', 'Our World in Data — Health Indicators', [
    dataRow('Life Expectancy',          sfx(owid(c,'lifeExpOwid'), 1, ' yrs'),      owidyr(c,'lifeExpOwid')),
    dataRow('Child Mortality',          sfx(owid(c,'childMortality'), 2, '%'),      owidyr(c,'childMortality')),
    dataRow('Maternal Mortality',       sfx(owid(c,'maternMortOwid'), 1, ' per 100k'), owidyr(c,'maternMortOwid')),
    dataRow('Diabetes Prevalence',      fmtPct(owid(c,'diabetesRate')),             owidyr(c,'diabetesRate')),
    dataRow('Cancer Death Rate',        sfx(owid(c,'cancerDeaths'), 1, ' per 100k'), owidyr(c,'cancerDeaths')),
    dataRow('Suicide Rate',             sfx(owid(c,'suicideRate'), 2, ' per 100k'), owidyr(c,'suicideRate')),
    dataRow('Alcohol Consumption',      sfx(owid(c,'alcoholConsump'), 1, ' L/yr'),  owidyr(c,'alcoholConsump')),
    dataRow('Homicide Rate',            sfx(owid(c,'homicideRate'), 2, ' per 100k'), owidyr(c,'homicideRate')),
    dataRow('Clean Fuels Access',       fmtPct(owid(c,'cleanFuels')),               owidyr(c,'cleanFuels')),
    dataRow('Safe Sanitation',          fmtPct(owid(c,'safeSanitation')),           owidyr(c,'safeSanitation')),
  ]);
}

function renderOWIDSociety(c) {
  return section('Society & Governance — OWID', 'owid', 'Our World in Data — Society', [
    dataRow('Corruption Index (0–100)', fmt(owid(c,'corruptionIndex'), 1),          owidyr(c,'corruptionIndex')),
    dataRow('Press Freedom Index',       fmt(owid(c,'pressFreedom'), 2),             owidyr(c,'pressFreedom')),
    dataRow('Women in Parliament',       fmtPct(owid(c,'womenInParl')),             owidyr(c,'womenInParl')),
    dataRow('Tourist Arrivals',          fmt(owid(c,'touristArrivals'), 0),          owidyr(c,'touristArrivals')),
    dataRow('Mean Years of Schooling',   sfx(owid(c,'meanSchoolingOwid'), 1, ' yrs'), owidyr(c,'meanSchoolingOwid')),
    dataRow('Gender Wage Gap',           fmtPct(owid(c,'genderWageGap')),           owidyr(c,'genderWageGap')),
    dataRow('Education Spend (% GDP)',   fmtPct(owid(c,'eduSpendOwid')),            owidyr(c,'eduSpendOwid')),
    dataRow('Extreme Poverty Rate',      fmtPct(owid(c,'extremePoverty')),          owidyr(c,'extremePoverty')),
    dataRow('Birth Rate',                sfx(owid(c,'birthRateOwid'), 1, ' per 1k'), owidyr(c,'birthRateOwid')),
  ]);
}

function renderOWIDFood(c) {
  return section('Food, Nature & Waste — OWID', 'owid', 'Our World in Data — Food & Environment', [
    dataRow('Daily Calories per Capita', sfx(owid(c,'dailyCalories'), 0, ' kcal'), owidyr(c,'dailyCalories')),
    dataRow('Meat Supply per Capita',    sfx(owid(c,'meatSupply'), 1, ' kg/yr'),   owidyr(c,'meatSupply')),
    dataRow('Forest Area',               fmtPct(owid(c,'forestOwid')),             owidyr(c,'forestOwid')),
    dataRow('Plastic Waste per Capita',  sfx(owid(c,'plasticWaste'), 3, ' kg/day'), owidyr(c,'plasticWaste')),
  ]);
}

// ── COMPONENT HELPERS ────────────────────────────────

function section(labelText, srcClass, title, rows) {
  const srcLabel = { wb: 'World Bank', imf: 'IMF', un: 'UN', oecd: 'OECD', owid: 'OWID', unesco: 'UNESCO', sipri: 'SIPRI' }[srcClass] || srcClass.toUpperCase();
  const content = rows.filter(Boolean).join('');
  return `
    <p class="section-label">${labelText} <span class="src-badge src-${srcClass}">${srcLabel}</span></p>
    <div class="data-section">
      <div class="data-section-header"><h3>${title}</h3></div>
      ${content || `<div class="no-data-row">No data available for this country</div>`}
    </div>`;
}

function statCard(label, value, sub, small = false) {
  return `<div class="stat-card">
    <div class="stat-label">${label}</div>
    <div class="stat-value" style="${small ? 'font-size:18px' : ''}">${value || '—'}</div>
    <div class="stat-sub">${sub || ''}</div>
  </div>`;
}

function dataRow(label, value, year) {
  if (value === null || value === undefined) return '';
  return `<div class="data-row">
    <span class="data-row-label">${label}</span>
    <div class="data-row-right">
      <div class="data-row-value">${value}</div>
      ${year ? `<div class="data-row-year">${year}</div>` : ''}
    </div>
  </div>`;
}

// NOTE: Add .src-unesco { background:#FFF8E8; color:#C07A00; } to styles.css

// ── UNESCO SECTIONS ───────────────────────────────────

function renderUNESCOEducation(c) {
  const hasFile = Object.keys(S.unescoData).length > 0;
  const rows = [
    dataRow('Adult Literacy Rate',          fmtPct(unesco(c,'literacyAdult')),           unescoyr(c,'literacyAdult')),
    dataRow('Youth Literacy Rate (15–24)',   fmtPct(unesco(c,'literacyYouth')),           unescoyr(c,'literacyYouth')),
    dataRow('Female Adult Literacy',         fmtPct(unesco(c,'literacyAdultF')),          unescoyr(c,'literacyAdultF')),
    dataRow('Primary Net Enrolment',         fmtPct(unesco(c,'primaryNetEnrol')),         unescoyr(c,'primaryNetEnrol')),
    dataRow('Primary Completion Rate',       fmtPct(unesco(c,'primaryCompletion')),       unescoyr(c,'primaryCompletion')),
    dataRow('Out-of-School Rate (Primary)',  fmtPct(unesco(c,'outOfSchoolPrimary')),      unescoyr(c,'outOfSchoolPrimary')),
    dataRow('Lower Secondary Completion',    fmtPct(unesco(c,'lowerSecCompletion')),      unescoyr(c,'lowerSecCompletion')),
    dataRow('Upper Secondary Completion',    fmtPct(unesco(c,'upperSecCompletion')),      unescoyr(c,'upperSecCompletion')),
    dataRow('Lower Sec. Net Enrolment',      fmtPct(unesco(c,'lowerSecNetEnrol')),        unescoyr(c,'lowerSecNetEnrol')),
    dataRow('Upper Sec. Net Enrolment',      fmtPct(unesco(c,'upperSecNetEnrol')),        unescoyr(c,'upperSecNetEnrol')),
    dataRow('Tertiary Gross Enrolment',      fmtPct(unesco(c,'tertiaryGrossEnrol')),      unescoyr(c,'tertiaryGrossEnrol')),
    dataRow('Tertiary Enrolment (Female)',   fmtPct(unesco(c,'tertiaryEnrolF')),          unescoyr(c,'tertiaryEnrolF')),
    dataRow('Pupil–Teacher Ratio (Primary)', fmt(unesco(c,'pupilTeacherPrimary'), 1),    unescoyr(c,'pupilTeacherPrimary')),
    dataRow('Pupil–Teacher Ratio (Sec.)',    fmt(unesco(c,'pupilTeacherSecondary'), 1),  unescoyr(c,'pupilTeacherSecondary')),
    dataRow('Trained Teachers, Primary (%)', fmtPct(unesco(c,'trainedTeachersPrimary')), unescoyr(c,'trainedTeachersPrimary')),
    dataRow('Trained Teachers, Sec. (%)',   fmtPct(unesco(c,'trainedTeachersSec')),     unescoyr(c,'trainedTeachersSec')),
    dataRow('Gender Parity — Primary',       fmt(unesco(c,'gpiPrimary'), 2),              unescoyr(c,'gpiPrimary')),
    dataRow('Gender Parity — Tertiary',      fmt(unesco(c,'gpiTertiary'), 2),             unescoyr(c,'gpiTertiary')),
    dataRow('Proficiency in Math (Primary)', fmtPct(unesco(c,'profMathPrimary')),        unescoyr(c,'profMathPrimary')),
    dataRow('Proficiency in Reading (Pri.)', fmtPct(unesco(c,'profReadPrimary')),        unescoyr(c,'profReadPrimary')),
    dataRow('Proficiency in Math (L.Sec.)', fmtPct(unesco(c,'profMathLowerSec')),       unescoyr(c,'profMathLowerSec')),
    dataRow('Govt Education Spend (% GDP)', fmtPct(unesco(c,'govtEduSpendGDP')),        unescoyr(c,'govtEduSpendGDP')),
    dataRow('Education Share of Budget',    fmtPct(unesco(c,'govtEduSpendBudget')),     unescoyr(c,'govtEduSpendBudget')),
    dataRow('Schools with Electricity',     fmtPct(unesco(c,'schoolsWithElec')),        unescoyr(c,'schoolsWithElec')),
    dataRow('Schools with Internet (Pri.)', fmtPct(unesco(c,'schoolsWithInternet')),    unescoyr(c,'schoolsWithInternet')),
    dataRow('Schools with Internet (Sec.)', fmtPct(unesco(c,'schoolsWithInternetSec')), unescoyr(c,'schoolsWithInternetSec')),
    dataRow('STEM Graduates (% of all)',    fmtPct(unesco(c,'stemGraduates')),           unescoyr(c,'stemGraduates')),
  ].filter(Boolean);

  const noDataMsg = hasFile
    ? `<div class="no-data-row">No UNESCO data for this country</div>`
    : `<div class="no-data-row">Run <code>python3 fetch_unesco.py</code> to load UNESCO data</div>`;

  return `
    <p class="section-label">Education — UNESCO <span class="src-badge src-unesco">UNESCO</span></p>
    <div class="data-section">
      <div class="data-section-header"><h3>UNESCO Education Indicators</h3></div>
      ${rows.join('') || noDataMsg}
    </div>`;
}

function renderUNESCOScience(c) {
  return section('Science & Innovation — UNESCO', 'unesco', 'UNESCO Science & Innovation', [
    dataRow('R&D Expenditure (% GDP)',        fmtPct(unesco(c,'rdSpendGDP')),            unescoyr(c,'rdSpendGDP')),
    dataRow('Govt R&D Spend (% GDP)',         fmtPct(unesco(c,'govRdGDP')),              unescoyr(c,'govRdGDP')),
    dataRow('Researchers (per million pop)',  fmt(unesco(c,'researchersPerMillion'), 0),  unescoyr(c,'researchersPerMillion')),
    dataRow('Female Researchers (%)',         fmtPct(unesco(c,'researchersFemale')),     unescoyr(c,'researchersFemale')),
    dataRow('Patent Applications (per mil)', fmt(unesco(c,'patentApplications'), 1),    unescoyr(c,'patentApplications')),
  ]);
}

// NOTE: Add these to styles.css:
//   .src-unesco { background:#FFF8E8; color:#C07A00; }
//   .src-sipri  { background:#FFF0F0; color:#B00020; }

// ── SIPRI SECTION ─────────────────────────────────────

function renderSIPRI(c) {
  return section('Military Expenditure — SIPRI', 'sipri', 'SIPRI Military Expenditure Database 1949–2024', [
    dataRow('Military Spend (% GDP)',        fmtPct(sipri(c,'milexPctGDP')),              sipriyr(c,'milexPctGDP')),
    dataRow('Military Spend (% Govt Budget)',fmtPct(sipri(c,'milexPctGovt')),             sipriyr(c,'milexPctGovt')),
    dataRow('Military Spend per Capita',     fmtUSD(sipri(c,'milexPerCapita')),           sipriyr(c,'milexPerCapita')),
    dataRow('Military Spend (current USD)',  sfx(sipri(c,'milexCurrentUSD'), 0, 'M'),     sipriyr(c,'milexCurrentUSD')),
    dataRow('Military Spend (2023 USD)',     sfx(sipri(c,'milexConstantUSD'), 0, 'M'),    sipriyr(c,'milexConstantUSD')),
  ]);
}

// ── INIT ─────────────────────────────────────────────
loadAllData();
