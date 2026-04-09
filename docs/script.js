/* ============================================
   LEUCEMIA PR — script.js
   ============================================ */

// ---- LOADER ----
(function () {
  document.body.classList.add('loading');

  const loader    = document.getElementById('loader');
  const bar       = document.getElementById('loaderBar');
  const lines     = document.getElementById('loaderLines');
  const labelEl   = document.getElementById('loaderLabel');

  const STEPS = [
    { pct: 8,   label: 'Conectando ao Databricks…',          log: '[INIT]  Spark session iniciada' },
    { pct: 20,  label: 'Lendo dataset DATASUS…',             log: '[READ]  leucemia_pr.csv · 148.392 linhas' },
    { pct: 34,  label: 'Carregando staging Bronze…',         log: '[STAGE] Bronze · Parquet gravado' },
    { pct: 50,  label: 'Aplicando transformações ETL…',      log: '[ETL]   Duplicatas removidas · −591 reg' },
    { pct: 63,  label: 'Populando Data Warehouse…',          log: '[DW]    fato_leucemia · 147.801 reg' },
    { pct: 75,  label: 'Executando pipeline ELT…',           log: '[ELT]   Silver → Gold concluído' },
    { pct: 88,  label: 'Rodando queries OLAP…',              log: '[OLAP]  5 queries · 0 erros' },
    { pct: 96,  label: 'Finalizando dashboard…',             log: '[DONE]  Pipeline OK · carregando UI' },
    { pct: 100, label: 'Pronto.',                            log: null },
  ];

  const DELAYS = [0, 320, 560, 820, 1100, 1380, 1620, 1860, 2150];

  function addLine(text) {
    const span = document.createElement('span');
    span.textContent = text;
    lines.appendChild(span);
    // keep max 5 visible lines
    while (lines.children.length > 5) lines.removeChild(lines.firstChild);
  }

  STEPS.forEach((step, i) => {
    setTimeout(() => {
      bar.style.width = step.pct + '%';
      labelEl.textContent = step.label;
      if (step.log) addLine(step.log);
    }, DELAYS[i]);
  });

  // dismiss after last step + short pause
  setTimeout(() => {
    loader.classList.add('done');
    document.body.classList.remove('loading');
  }, DELAYS[DELAYS.length - 1] + 520);
})();

// ---- THEME ----
const html = document.documentElement;
const themeBtn = document.getElementById('themeToggle');
const themeLabel = document.getElementById('themeLabel');

const saved = localStorage.getItem('lpr-theme') || 'light';
applyTheme(saved);

themeBtn.addEventListener('click', () => {
  const next = html.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
  applyTheme(next);
  localStorage.setItem('lpr-theme', next);
});

function applyTheme(t) {
  html.setAttribute('data-theme', t);
  themeLabel.textContent = t === 'light' ? 'Escuro' : 'Claro';
}

// ---- KPI COUNTERS ----
function animateCount(el, target, duration = 1600) {
  const start = performance.now();
  const fmt = n => Math.floor(n).toLocaleString('pt-BR');
  (function tick(now) {
    const p = Math.min((now - start) / duration, 1);
    const ease = 1 - Math.pow(1 - p, 3);
    el.textContent = fmt(target * ease);
    if (p < 1) requestAnimationFrame(tick);
    else el.textContent = fmt(target);
  })(start);
}

const kpiObs = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (!e.isIntersecting) return;
    const el = e.target;
    animateCount(el, parseInt(el.dataset.target, 10));
    kpiObs.unobserve(el);
  });
}, { threshold: 0.5 });

document.querySelectorAll('.kpi-n').forEach(el => kpiObs.observe(el));

// ---- TIMESTAMP ----
const logTs = document.getElementById('logTs');
if (logTs) logTs.textContent = new Date().toISOString().slice(0, 19);

// ---- PIPELINE LOG SIMULATION ----
const terminal = document.getElementById('terminal');
const runBtn = document.getElementById('runBtn');

const STEPS = [
  { text: '[OK]  Extract · Leitura do dataset ········ 148.392 reg', cls: 'ok', ms: 120 },
  { text: '[OK]  Staging · Bronze salvo ············· 148.392 reg', cls: 'ok', ms: 200 },
  { text: '[OK]  ETL · Remover duplicatas ··········· 147.801 reg  (−591)', cls: 'ok', ms: 280 },
  { text: '[OK]  ETL · Tratar nulos ················· 147.801 reg', cls: 'ok', ms: 180 },
  { text: '[OK]  ETL · Normalizar datas ············· 147.801 reg', cls: 'ok', ms: 200 },
  { text: '[OK]  DW · dim_municipio ················· 399 reg', cls: 'ok', ms: 260 },
  { text: '[OK]  DW · dim_diagnostico ··············· 12 reg', cls: 'ok', ms: 150 },
  { text: '[OK]  DW · dim_tempo ···················· 5.844 reg', cls: 'ok', ms: 170 },
  { text: '[OK]  DW · fato_leucemia ················ 147.801 reg', cls: 'ok', ms: 380 },
  { text: '[OK]  ELT · Bronze → Silver ············· 147.801 reg', cls: 'ok', ms: 300 },
  { text: '[OK]  ELT · Silver → Gold ··············· 147.801 reg', cls: 'ok', ms: 260 },
  { text: '[OK]  Validação final ·················· PASS', cls: 'ok', ms: 200 },
];

function runSimulation() {
  if (!terminal) return;
  terminal.innerHTML = '';
  runBtn.disabled = true;
  runBtn.textContent = 'rodando…';

  let delay = 0;
  STEPS.forEach((step, i) => {
    delay += step.ms;
    setTimeout(() => {
      const line = document.createElement('div');
      line.className = `t-line ${step.cls}`;
      line.textContent = step.text;
      terminal.appendChild(line);
      terminal.scrollTop = terminal.scrollHeight;

      if (i === STEPS.length - 1) {
        setTimeout(() => {
          const ts = document.createElement('div');
          ts.className = 't-line ts';
          ts.textContent = `Concluído · 0 falhas · ${new Date().toISOString().slice(0, 19)}`;
          terminal.appendChild(ts);

          const cursor = document.createElement('span');
          cursor.className = 't-cursor';
          cursor.textContent = '█';
          terminal.appendChild(cursor);

          terminal.scrollTop = terminal.scrollHeight;
          runBtn.disabled = false;
          runBtn.textContent = '↺ Simular run';
        }, 350);
      }
    }, delay);
  });
}

runBtn?.addEventListener('click', runSimulation);

// ---- BAR ANIMATION ----
const barObs = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (!e.isIntersecting) return;
    e.target.querySelectorAll('.bar-fill').forEach(bar => {
      const w = bar.style.getPropertyValue('--w');
      bar.style.width = '0';
      requestAnimationFrame(() => {
        setTimeout(() => { bar.style.width = w; }, 80);
      });
    });
    barObs.unobserve(e.target);
  });
}, { threshold: 0.2 });

document.querySelectorAll('.lake').forEach(el => barObs.observe(el));