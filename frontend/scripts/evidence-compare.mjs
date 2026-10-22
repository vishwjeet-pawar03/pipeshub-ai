// Compares two viewport-audit runs (tests/e2e/responsive) and writes a
// markdown table of each screen before and after.
// usage: node scripts/evidence-compare.mjs <before-dir> <after-dir> <out.md>
// Exits 1 when any screen in the after run still overflows, has clipped
// controls or page errors.
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';

/** Rows keyed by `id|viewport`; a later line for the same screen wins (re-runs append). */
function readMetrics(dir) {
  const file = join(dir, 'metrics.jsonl');
  const rows = new Map();
  if (!existsSync(file)) return rows;
  for (const line of readFileSync(file, 'utf-8').split('\n')) {
    if (!line.trim()) continue;
    const row = JSON.parse(line);
    rows.set(`${row.id}|${row.viewport}`, row);
  }
  return rows;
}

const problems = (row) =>
  !!row && (row.docOverflowPx > 0 || row.offscreenInteractive.length > 0 || row.pageErrors.length > 0 || !!row.actionError);

function cell(row, dir, outDir) {
  if (!row) return 'not run';
  const parts = [
    `overflow ${row.docOverflowPx}px`,
    `clipped controls ${row.offscreenInteractive.length}`,
    `errors ${row.pageErrors.length}`,
  ];
  if (row.actionError) parts.push('scenario failed');
  const image = relative(outDir, join(dir, `${row.id}--${row.viewport}.png`));
  return `${problems(row) ? '❌' : '✅'} ${parts.join(', ')} ([image](${image}))`;
}

const [beforeDir, afterDir, outFile] = process.argv.slice(2);
if (!beforeDir || !afterDir || !outFile) {
  console.error('usage: node scripts/evidence-compare.mjs <before-dir> <after-dir> <out.md>');
  process.exit(2);
}

const before = readMetrics(beforeDir);
const after = readMetrics(afterDir);
const viewportOrder = ['mobile', 'tablet', 'desktop'];
const keys = [...new Set([...before.keys(), ...after.keys()])].sort((a, b) => {
  const [ia, va] = a.split('|');
  const [ib, vb] = b.split('|');
  return ia.localeCompare(ib) || viewportOrder.indexOf(va) - viewportOrder.indexOf(vb);
});

const outDir = dirname(outFile);
const lines = ['| Screen | Viewport | Before | After |', '| --- | --- | --- | --- |'];
let failing = 0;
for (const key of keys) {
  const [id, viewport] = key.split('|');
  const a = after.get(key);
  if (problems(a)) failing++;
  lines.push(`| ${id} | ${viewport} | ${cell(before.get(key), beforeDir, outDir)} | ${cell(a, afterDir, outDir)} |`);
}
lines.push('', `${keys.length} screens; ${failing} still have problems after.`);
writeFileSync(outFile, `${lines.join('\n')}\n`);
console.log(`wrote ${outFile}: ${failing} of ${keys.length} screens still have problems`);
process.exit(failing > 0 ? 1 : 0);
