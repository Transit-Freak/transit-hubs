#!/usr/bin/env node
/*
 * process.js — server-side GTFS pre-processing for the static site.
 *
 * Runs in the weekly GitHub Action (see .github/workflows/refresh.yml), NOT in
 * the browser. It does exactly what the in-page app used to do on an uploaded
 * ZIP — parse stops/routes/stop_times/trips and reduce them to the per-stop
 * route lists the visualization needs — and writes the result to report.json.
 * The browser then just fetches that JSON instead of crunching a ~1GB feed.
 *
 * The parsing logic here is a faithful port of the component methods
 * (_passSmall / _passStopTimes / _passTrips / _combine) so the output is
 * identical to what the page produced from a manual upload.
 *
 * Usage: node process.js <gtfs.zip> [outBase]   ->   writes <outBase>.json
 *        (outBase defaults to "report")
 */
'use strict';

const fs = require('fs');
const fflate = require('fflate');

// ---------- geography helpers (ported verbatim from the page) ----------
function bbox() { return { laMin: 29.3, laMax: 32.30, loMin: 34.0, loMax: 35.70 }; }

const TOWNS = [
  { n: 'באר שבע', la: 31.252, lo: 34.791 }, { n: 'דימונה', la: 31.066, lo: 35.033 },
  { n: 'נתיבות', la: 31.422, lo: 34.589 }, { n: 'אופקים', la: 31.310, lo: 34.620 },
  { n: 'שדרות', la: 31.524, lo: 34.596 }, { n: 'קריית גת', la: 31.610, lo: 34.770 },
  { n: 'ערד', la: 31.259, lo: 35.213 }, { n: 'אילת', la: 29.557, lo: 34.952 },
  { n: 'מצפה רמון', la: 30.610, lo: 34.801 }, { n: 'ירוחם', la: 30.987, lo: 34.929 },
  { n: 'רהט', la: 31.393, lo: 34.754 }, { n: 'אשקלון', la: 31.668, lo: 34.574 },
  { n: 'שדה בוקר', la: 30.872, lo: 34.795 }, { n: 'תל שבע', la: 31.260, lo: 34.844 },
  { n: 'אשדוד', la: 31.802, lo: 34.656 }, { n: 'קריית מלאכי', la: 31.731, lo: 34.745 },
  { n: 'גדרה', la: 31.814, lo: 34.779 }, { n: 'רחובות', la: 31.894, lo: 34.812 },
  { n: 'נס ציונה', la: 31.929, lo: 34.799 }, { n: 'יבנה', la: 31.878, lo: 34.739 },
  { n: 'ראשון לציון', la: 31.964, lo: 34.804 }, { n: 'מודיעין', la: 31.898, lo: 35.010 },
  { n: 'בית שמש', la: 31.745, lo: 34.988 }, { n: 'קריית עקרון', la: 31.866, lo: 34.819 },
  { n: 'גן יבנה', la: 31.788, lo: 34.706 }, { n: 'אופקים', la: 31.310, lo: 34.620 },
  { n: 'מיתר', la: 31.319, lo: 34.933 }, { n: 'להבים', la: 31.371, lo: 34.814 },
  { n: 'שובל', la: 31.397, lo: 34.748 }, { n: 'באר טוביה', la: 31.737, lo: 34.721 },
  { n: 'קריית גת', la: 31.610, lo: 34.770 }, { n: 'יבנאל', la: 31.880, lo: 34.740 },
  { n: 'ערד', la: 31.259, lo: 35.213 }, { n: 'בית גוברין', la: 31.609, lo: 34.897 },
  { n: 'מסמיה', la: 31.738, lo: 34.808 }, { n: 'גני תקווה', la: 32.063, lo: 34.872 },
];
function nearestTown(la, lo) {
  let best = null, bd = Infinity; const cos = 0.853;
  for (const t of TOWNS) { const dla = la - t.la, dlo = (lo - t.lo) * cos; const d = dla * dla + dlo * dlo; if (d < bd) { bd = d; best = t.n; } }
  return bd < 0.0049 ? best : 'אזור פתוח';
}

function csv(line) {
  const out = []; let cur = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) { if (ch === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += ch; }
    else { if (ch === ',') { out.push(cur); cur = ''; } else if (ch === '"') q = true; else cur += ch; }
  }
  out.push(cur); return out;
}
function toInt(text, a, b) { let v = 0; for (let k = a; k < b; k++) { const c = text.charCodeAt(k); if (c >= 48 && c <= 57) v = v * 10 + (c - 48); } return v; }

// ---------- zip streaming ----------
// Mirror the page's _pushSliced: feed the in-memory zip to fflate.Unzip in
// chunks so per-file inflate is streamed (keeps stop_times.txt off the heap as
// one giant string). `wanted` maps a base filename to an ondata(text, final)
// line-buffered callback.
function streamZip(zip, wanted) {
  return new Promise((resolve, reject) => {
    const uz = new fflate.Unzip();
    uz.register(fflate.UnzipInflate);
    const dec = new TextDecoder('utf-8');
    const state = new Map(); // base -> { leftover }
    uz.onfile = (file) => {
      const base = file.name.split('/').pop();
      const cb = wanted[base];
      if (!cb) return;
      state.set(base, { leftover: '' });
      file.ondata = (err, data, final) => {
        if (err) { reject(err); return; }
        const st = state.get(base);
        let text = st.leftover + dec.decode(data, { stream: !final });
        let cut;
        if (final) { cut = text.length; st.leftover = ''; }
        else { cut = text.lastIndexOf('\n') + 1; st.leftover = text.slice(cut); }
        cb(text, cut, final);
      };
      file.start();
    };
    const SL = 4 * 1024 * 1024;
    let off = 0;
    try {
      while (off < zip.length) {
        const end = Math.min(off + SL, zip.length);
        uz.push(zip.subarray(off, end), end >= zip.length);
        off = end;
      }
      resolve();
    } catch (e) { reject(e); }
  });
}

// ---------- pass A: stops + routes ----------
async function passSmall(zip) {
  const parts = { 'stops.txt': [], 'routes.txt': [] };
  await streamZip(zip, {
    'stops.txt': (text, cut, final) => { parts['stops.txt'].push(final ? text : text.slice(0, cut)); },
    'routes.txt': (text, cut, final) => { parts['routes.txt'].push(final ? text : text.slice(0, cut)); },
  });
  // Reassemble with the leftover already folded back in by streamZip: because we
  // pushed slice(0,cut) per chunk and final pushes the whole tail, joining is safe.
  const stopsTxt = parts['stops.txt'].join('');
  const routesTxt = parts['routes.txt'].join('');
  const B = bbox();
  const sl = stopsTxt.split(/\r?\n/); const sh = csv(sl[0]);
  const iId = sh.indexOf('stop_id'), iN = sh.indexOf('stop_name'), iLa = sh.indexOf('stop_lat'), iLo = sh.indexOf('stop_lon');
  if (iId < 0 || iLa < 0 || iLo < 0) throw new Error('stops.txt invalid (missing required columns)');
  const keepSet = new Set(); const stopMeta = new Map();
  for (let k = 1; k < sl.length; k++) {
    const ln = sl[k]; if (!ln) continue; const c = csv(ln);
    const la = parseFloat(c[iLa]), lo = parseFloat(c[iLo]);
    if (!isFinite(la) || !isFinite(lo)) continue;
    if (la >= B.laMin && la <= B.laMax && lo >= B.loMin && lo <= B.loMax) {
      const id = parseInt(c[iId], 10); if (!isFinite(id)) continue;
      keepSet.add(id); stopMeta.set(id, { name: c[iN], lat: la, lon: lo, town: nearestTown(la, lo) });
    }
  }
  const rl = routesTxt.split(/\r?\n/); const rh = csv(rl[0]);
  const jR = rh.indexOf('route_id'), jS = rh.indexOf('route_short_name'), jL = rh.indexOf('route_long_name'), jA = rh.indexOf('agency_id'), jT = rh.indexOf('route_type');
  const routeMeta = new Map();
  for (let k = 1; k < rl.length; k++) {
    const ln = rl[k]; if (!ln) continue; const c = csv(ln);
    routeMeta.set(c[jR], { short: (c[jS] || '').trim(), long: (c[jL] || '').trim(), agency: jA >= 0 ? c[jA] : '', type: jT >= 0 ? c[jT] : '' });
  }
  return { keepSet, stopMeta, routeMeta };
}

// ---------- pass B: stop_times ----------
async function passStopTimes(zip, keepSet) {
  const stopTrips = new Map(); const keepTrips = new Set();
  let header = false, idxTrip = -1, idxStop = -1, rows = 0, lastRep = 0;
  await streamZip(zip, {
    'stop_times.txt': (text, cut, final) => {
      let i = 0;
      if (!header) {
        const nl = text.indexOf('\n'); const cols = text.slice(0, nl).split(',');
        idxTrip = cols.indexOf('trip_id'); idxStop = cols.indexOf('stop_id');
        if (idxTrip < 0 || idxStop < 0) throw new Error('unexpected stop_times.txt structure');
        header = true; i = nl + 1;
      }
      const maxIdx = Math.max(idxTrip, idxStop);
      while (i < cut) {
        let nl = text.indexOf('\n', i); if (nl < 0 || nl > cut) nl = cut;
        let col = 0, fStart = i, tStart = -1, tEnd = -1, sidVal = 0, sidHas = false;
        for (let j = i; j < nl; j++) {
          if (text.charCodeAt(j) === 44) {
            if (col === idxTrip) { tStart = fStart; tEnd = j; }
            else if (col === idxStop) { sidVal = toInt(text, fStart, j); sidHas = true; }
            col++; fStart = j + 1;
            if (col > maxIdx && tStart >= 0 && sidHas) break;
          }
        }
        if (col === idxTrip && tStart < 0) { tStart = fStart; tEnd = nl; }
        if (col === idxStop && !sidHas) { sidVal = toInt(text, fStart, nl); sidHas = true; }
        if (sidHas && keepSet.has(sidVal) && tStart >= 0) {
          const tid = text.slice(tStart, tEnd);
          let s = stopTrips.get(sidVal); if (!s) { s = new Set(); stopTrips.set(sidVal, s); }
          s.add(tid); keepTrips.add(tid);
        }
        rows++; i = nl + 1;
      }
      if (rows - lastRep > 1000000) { lastRep = rows; process.stderr.write('  stop_times: ' + (rows / 1e6).toFixed(1) + 'M rows\n'); }
    },
  });
  return { stopTrips, keepTrips };
}

// ---------- pass C: trips ----------
async function passTrips(zip, keepTrips) {
  const tripRoute = new Map(); const svc = new Map();
  let header = false, iR = -1, iS = -1, iT = -1, rows = 0, lastRep = 0;
  await streamZip(zip, {
    'trips.txt': (text, cut, final) => {
      let i = 0;
      if (!header) {
        const nl = text.indexOf('\n'); const cols = text.slice(0, nl).split(',');
        iR = cols.indexOf('route_id'); iS = cols.indexOf('service_id'); iT = cols.indexOf('trip_id');
        if (iR < 0 || iS < 0 || iT < 0) throw new Error('unexpected trips.txt structure');
        header = true; i = nl + 1;
      }
      const maxIdx = Math.max(iR, iS, iT);
      while (i < cut) {
        let nl = text.indexOf('\n', i); if (nl < 0 || nl > cut) nl = cut;
        let col = 0, fStart = i; let rS = -1, rE = -1, sS = -1, sE = -1, tS = -1, tE = -1;
        for (let j = i; j < nl; j++) {
          if (text.charCodeAt(j) === 44) {
            if (col === iR) { rS = fStart; rE = j; } else if (col === iS) { sS = fStart; sE = j; } else if (col === iT) { tS = fStart; tE = j; }
            col++; fStart = j + 1; if (col > maxIdx) break;
          }
        }
        if (col === iR && rS < 0) { rS = fStart; rE = nl; }
        if (col === iS && sS < 0) { sS = fStart; sE = nl; }
        if (col === iT && tS < 0) { tS = fStart; tE = nl; }
        if (tS >= 0) {
          const tid = text.slice(tS, tE);
          if (keepTrips.has(tid)) {
            const rid = text.slice(rS, rE); const sid = text.slice(sS, sE);
            tripRoute.set(tid, rid);
            let m = svc.get(rid); if (!m) { m = new Map(); svc.set(rid, m); }
            m.set(sid, (m.get(sid) || 0) + 1);
          }
        }
        rows++; i = nl + 1;
      }
      if (rows - lastRep > 1000000) { lastRep = rows; process.stderr.write('  trips: ' + (rows / 1e6).toFixed(1) + 'M rows\n'); }
    },
  });
  const routeDaily = new Map();
  svc.forEach((m, rid) => { let mx = 0; m.forEach(v => { if (v > mx) mx = v; }); routeDaily.set(rid, mx); });
  return { tripRoute, routeDaily };
}

// ---------- combine (mirrors _combine) ----------
function combine(stopMeta, routeMeta, stopTrips, tripRoute, routeDaily) {
  const stops = [];
  stopTrips.forEach((tripSet, sid) => {
    const meta = stopMeta.get(sid); if (!meta) return;
    const rset = new Set();
    tripSet.forEach(tid => { const r = tripRoute.get(tid); if (r) rset.add(r); });
    if (rset.size === 0) return;
    stops.push({ id: sid, name: meta.name, lat: meta.lat, lon: meta.lon, town: meta.town, routes: Array.from(rset) });
  });
  return { stops, routeMeta, routeDaily };
}

async function main() {
  const zipPath = process.argv[2];
  const outBase = process.argv[3] || 'report';
  if (!zipPath) { console.error('usage: node process.js <gtfs.zip> [outBase]'); process.exit(1); }
  process.stderr.write('Reading ' + zipPath + '…\n');
  const zip = new Uint8Array(fs.readFileSync(zipPath));

  process.stderr.write('Pass A: stops + routes…\n');
  const { keepSet, stopMeta, routeMeta } = await passSmall(zip);
  if (keepSet.size === 0) throw new Error('no stops found in the south/center bounding box');
  process.stderr.write('  kept ' + keepSet.size + ' stops, ' + routeMeta.size + ' routes\n');

  process.stderr.write('Pass B: stop_times…\n');
  const { stopTrips, keepTrips } = await passStopTimes(zip, keepSet);

  process.stderr.write('Pass C: trips…\n');
  const { tripRoute, routeDaily } = await passTrips(zip, keepTrips);

  const raw = combine(stopMeta, routeMeta, stopTrips, tripRoute, routeDaily);

  // Serialize Maps as plain objects; the browser rebuilds them on load.
  const report = {
    generatedAt: new Date().toISOString().slice(0, 10),
    stops: raw.stops,
    routeMeta: Object.fromEntries(raw.routeMeta),
    routeDaily: Object.fromEntries(raw.routeDaily),
  };
  const out = outBase + '.json';
  fs.writeFileSync(out, JSON.stringify(report));
  process.stderr.write('Wrote ' + out + ' — ' + raw.stops.length + ' stops with routes, ' +
    Object.keys(report.routeMeta).length + ' routes, generatedAt ' + report.generatedAt + '\n');
}

main().catch(err => { console.error('process.js failed:', err && err.stack || err); process.exit(1); });
