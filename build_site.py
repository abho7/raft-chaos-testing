"""
Generate the static results report.

Reads site/results.json and writes a single self-contained site/index.html
with the data inlined. Inlining rather than fetching means the report opens
correctly from the filesystem as well as from GitHub Pages -- a fetch() of a
sibling JSON file is blocked under file://.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "site" / "results.json"
OUT = ROOT / "site" / "index.html"


# The findings narrative. Written by hand because it is the analysis, not
# something derivable from the run: the harness found no engine defect, and
# saying so plainly is the finding.
FINDINGS = {
    "engine_bugs": [],
    "harness_bugs": [
        {
            "title": "Acknowledgements ordered by detection time, not log index",
            "severity": "false positive",
            "found_by": "randomized sweep, seed 124",
            "symptom": "50 reported acked-read violations against entirely correct Raft behaviour.",
            "detail": (
                "An entry whose proposing leader was partitioned is only noticed as committed "
                "once that node rejoins. Under seed 124 index 3 (c='30') was therefore detected "
                "at tick 250, after index 4 (c='24') had already been detected at tick 248. The "
                "checker recorded the newest acknowledged value per key in detection order, so "
                "it regressed its expectation to a value that a later write had legitimately "
                "superseded, then flagged the leader for disagreeing with it."
            ),
            "resolution": (
                "Expected values are now tracked by log index rather than detection order, and "
                "an entry is recognised as committed via any alive node whose commit_index "
                "covers it, rather than only via its original proposer."
            ),
            "test": "tests/test_harness.py::test_seed_124_does_not_report_a_false_acked_read_violation",
        },
        {
            "title": "Timeline silently erased overwritten writes",
            "severity": "reporting defect",
            "found_by": "manual review of scenario fidelity",
            "symptom": "The single most interesting event in 'leader killed mid-write' was missing from its own timeline.",
            "detail": (
                "Derived write records were keyed by log index. When a leader is unseated "
                "mid-write, the next leader reuses that index for a different command, so the "
                "in-flight entry's record was overwritten by its replacement -- exactly the "
                "event the scenario exists to demonstrate."
            ),
            "resolution": "Writes are kept as an ordered list; acknowledgements match on (index, term), which is unique.",
            "test": "Covered indirectly by the scenario timelines; see kill-leader-mid-write below.",
        },
        {
            "title": "A scenario tested the opposite of its description",
            "severity": "vacuous pass",
            "found_by": "manual review of scenario fidelity",
            "symptom": "'The majority must keep committing' was asserted while writing to the isolated minority.",
            "detail": (
                "propose() scans nodes in id order and hands the write to the first node that "
                "believes it is leader. During a partition that is often the isolated stale "
                "leader, whose write correctly never commits -- so the scenario passed while "
                "demonstrating nothing about the majority side."
            ),
            "resolution": "Added propose_to() and leader_within(), so any scenario asserting something about a specific side must name that side.",
            "test": "tests/test_harness.py::test_propose_to_refuses_a_node_that_is_not_leader",
        },
    ],
}


def build_html(data: dict) -> str:
    # The page renders derived views (leader bands, fault windows, writes),
    # never the raw event stream -- and that stream is most of the bytes. Drop
    # it from the inlined copy; results.json beside this file keeps the full
    # record for anyone who wants to re-analyse a run.
    slim = {
        **data,
        "scenarios": [{k: v for k, v in sc.items() if k != "events"} for sc in data["scenarios"]],
        "findings": FINDINGS,
    }
    payload = json.dumps(slim, separators=(",", ":"))
    return TEMPLATE.replace("__DATA__", payload)


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Raft Chaos Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
/* ---------------------------------------------------------------------------
   PALETTE -- committed dark. This report is telemetry, not a document, so it
   ships one look rather than reacting to the OS theme. Every colour is painted
   explicitly; nothing inherits from a host surface.

   One accent hue (cyan) carries identity, one status hue (red) carries fault
   state. Two steps of cyan, deliberately:

     --accent  #22d3ee  text, headline, glows. TEXT scope -- held to WCAG
                        contrast (10.83 on the page), not the mark band.
     --leader  #0aa5c0  filled data bands. MARK scope -- the lightest cyan
                        that sits inside the dark lightness band (L 0.665).

   Data marks validated together on #0b0c0e: all six checks pass, worst-pair
   CVD dE 15.4 (deutan), normal-vision dE 32.8. A brighter cyan was tried for
   the bands and rejected -- #22d3ee measures L 0.797, outside the band.
--------------------------------------------------------------------------- */
:root {
  color-scheme: dark;
  --page:     #0b0c0e;
  --panel:    #121417;
  --panel-2:  #16191d;
  --raised:   #1b1f24;
  --line:     #23272d;
  --line-2:   #2d323a;

  --ink:      #f2f4f6;
  --ink-2:    #a8b0b8;
  --label:    #7c858e;

  --accent:   #22d3ee;
  --accent-d: #0aa5c0;
  --leader:   #0aa5c0;
  --fault:    #f43f5e;
  --ok:       #22d3ee;

  --sans: "Space Grotesk", ui-sans-serif, -apple-system, "Segoe UI", system-ui, sans-serif;
  --mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Consolas, monospace;

  --shadow: 0 1px 2px rgba(0,0,0,.6), 0 8px 24px -12px rgba(0,0,0,.8);
  --glow: 0 0 16px rgba(34,211,238,.28);
}

* { box-sizing: border-box; }
html { overflow-x: hidden; overflow-x: clip; }
body {
  margin: 0; overflow-x: hidden; overflow-x: clip;
  background: var(--page);
  color: var(--ink);
  font-family: var(--sans);
  font-size: 15.5px; line-height: 1.6;
  -webkit-font-smoothing: antialiased;
  /* faint instrument-panel wash, not a texture */
  background-image:
    radial-gradient(900px 500px at 12% -8%, rgba(34,211,238,.06), transparent 60%),
    radial-gradient(700px 420px at 92% 4%, rgba(244,63,94,.045), transparent 60%);
  background-attachment: fixed;
}
main { max-width: 68rem; margin: 0 auto; padding: clamp(2rem,5vw,3.5rem) clamp(1rem,3vw,2rem) 6rem; }

/* ------------------------------------------------------------------ chrome */
.rail {
  display: flex; align-items: center; gap: .7rem;
  font-family: var(--mono); font-size: .72rem; letter-spacing: .04em;
  color: var(--label); border-bottom: 1px solid var(--line);
  padding-bottom: .8rem; margin-bottom: clamp(2rem,5vw,3rem);
  flex-wrap: wrap;
}
.rail .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--accent); box-shadow: var(--glow); }
.rail .sep { color: var(--line-2); }
.rail a { color: var(--ink-2); text-decoration: none; border-bottom: 1px solid var(--line-2); }
.rail a:hover { color: var(--accent); }
.rail .right { margin-left: auto; }

h1 {
  font-family: var(--sans); font-weight: 700;
  font-size: clamp(1.9rem,4.6vw,2.9rem); line-height: 1.06; letter-spacing: -.03em;
  margin: 0 0 .55rem;
}
.lede { color: var(--ink-2); margin: 0; max-width: 58ch; font-size: clamp(.95rem,1.6vw,1.05rem); }

.kicker {
  font-family: var(--mono); font-size: .68rem; font-weight: 500;
  letter-spacing: .18em; text-transform: uppercase; color: var(--accent);
  margin: 0 0 .8rem;
}
.kicker.dim { color: var(--label); }

/* ------------------------------------------------------------- headline hit */
.headline {
  margin-top: clamp(2rem,4vw,3rem);
  border: 1px solid var(--line);
  border-radius: 18px;
  background:
    linear-gradient(180deg, rgba(34,211,238,.055), transparent 42%),
    var(--panel);
  box-shadow: var(--shadow);
  padding: clamp(1.6rem,4vw,2.6rem);
  position: relative; overflow: hidden;
}
/* thin live scan line along the top edge */
.headline::before {
  content:""; position:absolute; inset:0 0 auto 0; height:1px;
  background: linear-gradient(90deg, transparent, var(--accent), transparent);
  opacity:.55; animation: scan 5.5s ease-in-out infinite;
}
@keyframes scan { 0%,100%{transform:translateX(-40%)} 50%{transform:translateX(40%)} }

.stat-hero {
  font-family: var(--mono); font-weight: 700;
  font-size: clamp(3rem,11vw,6.2rem); line-height: .92; letter-spacing: -.045em;
  color: var(--accent); text-shadow: 0 0 34px rgba(34,211,238,.34);
  margin: .1rem 0 .1rem;
}
.stat-hero .slash { color: var(--line-2); }
.stat-sub {
  font-family: var(--mono); font-size: .78rem; letter-spacing: .16em;
  text-transform: uppercase; color: var(--ink-2); margin: 0 0 1.4rem;
}
.stat-note { color: var(--ink-2); margin: 0; max-width: 62ch; font-size: .93rem; }

.readout {
  display: grid; gap: 1px; margin-top: 1.8rem;
  grid-template-columns: repeat(auto-fit, minmax(7.6rem,1fr));
  background: var(--line); border: 1px solid var(--line); border-radius: 12px; overflow: hidden;
}
.cell { background: var(--panel-2); padding: .85rem .95rem; }
.cell .k { font-family: var(--mono); font-size: .62rem; letter-spacing: .13em; text-transform: uppercase; color: var(--label); }
.cell .v { font-family: var(--mono); font-size: 1.28rem; font-weight: 700; margin-top: .2rem; letter-spacing: -.02em; }
.cell .v.hot { color: var(--fault); }

/* -------------------------------------------------------------------- prose */
section { margin-top: clamp(2.8rem,6vw,4.2rem); }
h2 { font-weight: 600; font-size: clamp(1.2rem,2.6vw,1.5rem); letter-spacing: -.02em; margin: 0 0 .9rem; }
.prose p, .prose li { color: var(--ink-2); max-width: 68ch; }
.prose strong { color: var(--ink); font-weight: 600; }
.prose ul { padding-left: 1.05rem; }
.prose li { margin: .35rem 0; }
code, .mono { font-family: var(--mono); font-size: .84em; }
code { background: var(--raised); border: 1px solid var(--line); padding: .1em .38em; border-radius: 5px; color: var(--ink); }

.checklist { display: grid; gap: .55rem; margin-top: 1rem; }
.check {
  display: grid; grid-template-columns: auto 1fr; gap: .75rem; align-items: start;
  background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: .8rem 1rem;
}
.check .num { font-family: var(--mono); font-size: .72rem; color: var(--accent); padding-top: .18rem; }
.check b { font-weight: 600; }
.check span { color: var(--ink-2); font-size: .92rem; }

/* ----------------------------------------------------------------- findings */
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 14px; box-shadow: var(--shadow); }
.clean {
  padding: 1.4rem 1.5rem; border-left: 2px solid var(--accent);
  background: linear-gradient(90deg, rgba(34,211,238,.05), transparent 30%), var(--panel);
}
.clean p { color: var(--ink-2); margin: .45rem 0; max-width: 70ch; }

.finding { padding: 1.35rem 1.5rem; margin-top: .85rem; border-left: 2px solid var(--fault); }
.finding h3 { font-size: 1rem; font-weight: 600; margin: .1rem 0 .55rem; }
.chips { display: flex; gap: .4rem; flex-wrap: wrap; margin-bottom: .55rem; }
.chip {
  font-family: var(--mono); font-size: .62rem; letter-spacing: .1em; text-transform: uppercase;
  color: var(--label); border: 1px solid var(--line-2); border-radius: 999px; padding: .18rem .55rem;
}
.chip.hot { color: var(--fault); border-color: rgba(244,63,94,.4); }
.finding p { color: var(--ink-2); margin: .45rem 0; max-width: 72ch; font-size: .93rem; }
.finding .fix { color: var(--ink); }
.finding .test { font-family: var(--mono); font-size: .72rem; color: var(--label); word-break: break-all; }

/* ---------------------------------------------------------------- scenarios */
.scenario { margin-top: .7rem; overflow: hidden; transition: border-color .2s ease, box-shadow .2s ease; }
.scenario:hover { border-color: var(--line-2); }
.scenario[open] { border-color: rgba(34,211,238,.28); box-shadow: var(--shadow), 0 0 0 1px rgba(34,211,238,.06); }
.scenario > summary { list-style: none; cursor: pointer; padding: 1rem 1.25rem; display: flex; align-items: center; gap: 1rem; }
.scenario > summary::-webkit-details-marker { display: none; }
.scenario > summary:focus-visible { outline: 2px solid var(--accent); outline-offset: -2px; }
.sum-id { font-family: var(--mono); font-size: .7rem; color: var(--label); min-width: 1.6rem; }
.sum-main { flex: 1; min-width: 0; }
.sum-name { font-weight: 600; letter-spacing: -.01em; }
.sum-fault { font-family: var(--mono); font-size: .72rem; color: var(--label); margin-top: .18rem;
             overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.verdict {
  font-family: var(--mono); font-size: .66rem; font-weight: 700; letter-spacing: .12em; text-transform: uppercase;
  padding: .28rem .6rem; border-radius: 6px; white-space: nowrap;
}
.verdict.pass { color: var(--accent); background: rgba(34,211,238,.09); border: 1px solid rgba(34,211,238,.28); }
.verdict.fail { color: var(--fault); background: rgba(244,63,94,.10); border: 1px solid rgba(244,63,94,.35); }
.caret { color: var(--line-2); font-family: var(--mono); transition: transform .22s cubic-bezier(.2,.7,.3,1); }
.scenario[open] .caret { transform: rotate(90deg); color: var(--accent); }

/* Smooth expand: 0fr -> 1fr on a grid row animates height without a fixed max. */
.body-wrap { display: grid; grid-template-rows: 0fr; transition: grid-template-rows .28s cubic-bezier(.2,.7,.3,1); }
.scenario[open] .body-wrap { grid-template-rows: 1fr; }
.body-inner { overflow: hidden; }
.scenario-body { padding: .3rem 1.25rem 1.4rem; border-top: 1px solid var(--line); margin-top: .2rem; }
.scenario-body > :first-child { margin-top: 1.1rem; }
.desc { color: var(--ink-2); max-width: 72ch; font-size: .93rem; }

/* ----------------------------------------------------------------- timeline */
.tl { margin-top: 1.2rem; background: var(--panel-2); border: 1px solid var(--line); border-radius: 12px; padding: 1rem 1.1rem .9rem; }
.tl-row { display: grid; grid-template-columns: 3.4rem 1fr; align-items: center; gap: .7rem; margin-bottom: 4px; }
.tl-name { font-family: var(--mono); font-size: .7rem; color: var(--ink-2); text-align: right; }
.tl-name.fault-name { color: var(--fault); }
.tl-track { position: relative; height: 18px; background: var(--raised); border-radius: 3px; border: 1px solid var(--line); }

.band {
  position: absolute; top: 0; height: 100%; border-radius: 3px;
  display: flex; align-items: center; padding: 0 .4rem;
  font-family: var(--mono); font-size: .62rem; font-weight: 500;
  /* Ellipsis rather than a hard clip: a truncated label like "unseat n3 ("
     reads as a rendering fault. The full text stays in the title tooltip. */
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  animation: grow .5s cubic-bezier(.2,.8,.25,1) both;
  transform-origin: left center;
}
@keyframes grow { from { transform: scaleX(.02); opacity: 0 } to { transform: scaleX(1); opacity: 1 } }

.band.leader {
  background: linear-gradient(180deg, var(--accent-d), #0b8ba3);
  color: #04222a;
  box-shadow: 0 0 14px rgba(34,211,238,.30), inset 0 1px 0 rgba(255,255,255,.18);
}
/* the band still open at the end of the run reads as live */
.band.leader.live { animation: grow .5s cubic-bezier(.2,.8,.25,1) both, breathe 3.4s ease-in-out .5s infinite; }
@keyframes breathe {
  0%,100% { box-shadow: 0 0 14px rgba(34,211,238,.30), inset 0 1px 0 rgba(255,255,255,.18); }
  50%     { box-shadow: 0 0 24px rgba(34,211,238,.55), inset 0 1px 0 rgba(255,255,255,.22); }
}
.band.fault {
  background: repeating-linear-gradient(135deg, rgba(244,63,94,.95) 0 7px, rgba(215,45,75,.95) 7px 14px);
  color: #fff; box-shadow: inset 0 1px 0 rgba(255,255,255,.14);
}

/* The instant a fault is injected. The core is light rather than red: this
   dot sits on top of the red fault band it starts, so a red core would be
   invisible against its own hatching. */
.inject { position: absolute; top: 50%; width: 7px; height: 7px; margin: -3.5px 0 0 -3.5px; border-radius: 50%;
          background: #fff; border: 1px solid rgba(0,0,0,.45);
          box-shadow: 0 0 0 0 rgba(255,255,255,.75); animation: ping 2.6s ease-out infinite; z-index: 2; }
@keyframes ping {
  0%   { box-shadow: 0 0 0 0 rgba(255,255,255,.7); }
  70%  { box-shadow: 0 0 0 10px rgba(255,255,255,0); }
  100% { box-shadow: 0 0 0 0 rgba(255,255,255,0); }
}

.tl-marks-row { display: grid; grid-template-columns: 3.4rem 1fr; gap: .7rem; margin-top: .35rem; }
.tl-marks { position: relative; height: 30px; }
/* Write markers wear ink and shape, never a data hue -- acked is filled,
   unacknowledged is hollow, and both carry a label, so the distinction
   survives greyscale and any colour-vision difference. */
.mark { position: absolute; transform: translateX(-50%); text-align: center; }
.pip { width: 9px; height: 9px; border-radius: 50%; margin: 0 auto; border: 1.5px solid var(--ink); background: transparent; }
.pip.acked { background: var(--ink); }
.mark .cap { font-family: var(--mono); font-size: .6rem; color: var(--label); margin-top: 3px; }

.axis { display: grid; grid-template-columns: 3.4rem 1fr; gap: .7rem; }
.axis .t { display: flex; justify-content: space-between; font-family: var(--mono); font-size: .62rem; color: var(--label); }

.key { display: flex; gap: 1rem; flex-wrap: wrap; font-family: var(--mono); font-size: .68rem; color: var(--ink-2); margin-top: .9rem;
       padding-top: .8rem; border-top: 1px solid var(--line); }
.key span { display: inline-flex; align-items: center; gap: .4rem; }
.key i { width: 14px; height: 9px; border-radius: 2px; display: inline-block; }
.key i.leader { background: var(--accent-d); box-shadow: 0 0 8px rgba(34,211,238,.4); }
.key i.fault { background: repeating-linear-gradient(135deg, rgba(244,63,94,.95) 0 4px, rgba(215,45,75,.95) 4px 8px); }
.key .pip { width: 9px; height: 9px; }

/* ------------------------------------------------------------------- table */
.tbl-wrap { overflow-x: auto; margin-top: 1.1rem; border: 1px solid var(--line); border-radius: 10px; }
table { border-collapse: collapse; width: 100%; font-size: .8rem; min-width: 34rem; }
thead { background: var(--raised); }
th, td { text-align: left; padding: .5rem .75rem; border-bottom: 1px solid var(--line); }
tbody tr:last-child td { border-bottom: 0; }
th { font-family: var(--mono); font-size: .62rem; letter-spacing: .1em; text-transform: uppercase; color: var(--label); font-weight: 500; }
td { font-family: var(--mono); color: var(--ink-2); }
td.cmd { color: var(--ink); }
td.ok { color: var(--accent); }
td.no { color: var(--label); }

.counters { display: flex; gap: 1.2rem; flex-wrap: wrap; margin-top: 1.1rem;
            font-family: var(--mono); font-size: .7rem; color: var(--label); }
.counters b { color: var(--ink); font-weight: 500; }

footer { margin-top: 5rem; padding-top: 1.2rem; border-top: 1px solid var(--line);
         font-family: var(--mono); font-size: .7rem; color: var(--label); }
footer a { color: var(--ink-2); }

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation: none !important; transition: none !important; }
}
@media (max-width: 34rem) {
  .tl-row, .tl-marks-row, .axis { grid-template-columns: 2.4rem 1fr; }
}
</style>
</head>
<body>
<main>

  <div class="rail">
    <span class="dot"></span>
    <span>RAFT-CHAOS</span>
    <span class="sep">/</span>
    <span id="rail-engine"></span>
    <span class="right" id="rail-time"></span>
  </div>

  <header>
    <p class="kicker">Fault injection · correctness verification</p>
    <h1>Raft under chaos</h1>
    <p class="lede">Composable fault injection and per-tick correctness verification against a Raft
      consensus implementation, driven externally through a queued transport built on its own node API.</p>
  </header>

  <div class="headline">
    <p class="kicker">Verdict</p>
    <p class="stat-hero" id="hero"></p>
    <p class="stat-sub" id="hero-sub"></p>
    <p class="stat-note" id="hero-note"></p>
    <div class="readout" id="readout"></div>
  </div>

  <section class="prose">
    <p class="kicker dim">Method</p>
    <h2>What is checked, and what is not</h2>
    <p>Four properties are asserted on <strong>every tick</strong> rather than at the end of a run, so the
      report can say when something broke rather than only that the final state looked healthy. A violation
      that appears and then heals still counts.</p>
    <div class="checklist" id="checklist"></div>
    <p style="margin-top:1.2rem">A write counts as <strong>acknowledged</strong> only once the leader's
      <code>commit_index</code> covers it — the moment a real server would answer the client. An uncommitted
      entry disappearing is correct Raft behaviour and is recorded as an overwrite, never as a loss.</p>
    <p>Stale reads from followers are deliberately <strong>not</strong> treated as violations. The engine
      documents that reads are served locally with no read-index or leader lease, so a lagging follower
      returning an old value is designed behaviour rather than a defect.</p>
  </section>

  <section>
    <p class="kicker dim">Findings</p>
    <h2 id="findings-title"></h2>
    <div id="findings"></div>
  </section>

  <section>
    <p class="kicker dim">Scenarios</p>
    <h2>What was injected, and what happened</h2>
    <p class="desc">Timelines run left to right in simulation ticks. Cyan bands are leadership, hatched red
      bands are active faults, and the pulsing dot marks the instant a fault was injected.</p>
    <div id="scenarios"></div>
  </section>

  <footer id="footer"></footer>
</main>

<script id="payload" type="application/json">__DATA__</script>
<script>
const DATA = JSON.parse(document.getElementById("payload").textContent);
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const pct = (v, total) => total > 0 ? (v / total) * 100 : 0;

const s = DATA.summary;
const clean = s.violations === 0;

/* ------------------------------------------------------------------- chrome */
$("rail-engine").innerHTML =
  `engine <a href="${esc(DATA.engine.repo)}">${esc(DATA.engine.repo.replace("https://github.com/",""))}</a> · read-only`;
$("rail-time").textContent = DATA.generated_at;

/* ----------------------------------------------------------------- headline */
$("hero").innerHTML = `${s.passed}<span class="slash">/</span>${s.scenarios}`;
$("hero-sub").textContent = clean ? "scenarios verified · zero safety violations" : "scenarios passed · violations detected";
$("hero-note").textContent = clean
  ? `Across every named scenario and a 600-seed randomized sweep covering 1,552 acknowledged writes, no acknowledged write was lost, no two nodes diverged at a log index, and no term ever had two leaders.`
  : `${s.failed} of ${s.scenarios} scenarios reported at least one violation. Details below.`;

$("readout").innerHTML = [
  ["scenarios", s.scenarios, false],
  ["passed", s.passed, false],
  ["violations", s.violations, s.violations > 0],
  ["acked writes", s.acked_writes, false],
  ["messages", s.total_messages.toLocaleString(), false],
  ["dropped", s.total_dropped.toLocaleString(), false],
  ["sim ticks", s.total_ticks.toLocaleString(), false],
].map(([k, v, hot]) =>
  `<div class="cell"><div class="k">${esc(k)}</div><div class="v${hot ? " hot" : ""}">${esc(v)}</div></div>`).join("");

$("checklist").innerHTML = [
  ["01", "Election safety", "At most one leader per term."],
  ["02", "State machine safety", "No two nodes ever apply different commands at the same log index."],
  ["03", "Leader completeness", "Every acknowledged entry is present, unchanged, in the log of every leader elected afterwards."],
  ["04", "Acked read consistency", "Once the leader has applied up to an acknowledged write, reading that key from the leader returns that value."],
].map(([n, t, d]) =>
  `<div class="check"><span class="num">${n}</span><span><b>${esc(t)}</b> — ${esc(d)}</span></div>`).join("");

/* ----------------------------------------------------------------- findings */
const f = DATA.findings;
$("findings-title").textContent = f.engine_bugs.length
  ? `${f.engine_bugs.length} defect(s) found in the engine`
  : "No defect found in the engine";

let html = "";
if (!f.engine_bugs.length) {
  html += `<div class="panel clean">
    <p><strong>The engine came through clean.</strong> Six hand-built scenarios and 600 randomized fault
    schedules covering 1,552 acknowledged writes produced no election-safety, state-machine-safety, or
    leader-completeness violation.</p>
    <p>That is a negative result, and its limits are worth stating precisely: it means no violation was found
    by these faults, not that none exists. The engine also runs without disk persistence, so a real process
    crash losing its term and vote is outside what this harness can model.</p>
  </div>`;
}
if (f.harness_bugs.length) {
  html += `<p class="desc" style="margin-top:1.6rem;max-width:72ch">Every defect this exercise surfaced was in
    the <strong style="color:var(--ink)">harness</strong>, not the engine. They are documented here because a
    chaos report that hides its own false positives is not worth reading — two of these would have produced a
    confidently wrong bug report.</p>`;
  html += f.harness_bugs.map(b => `
    <div class="panel finding">
      <div class="chips">
        <span class="chip hot">${esc(b.severity)}</span>
        <span class="chip">${esc(b.found_by)}</span>
      </div>
      <h3>${esc(b.title)}</h3>
      <p><strong>Symptom.</strong> ${esc(b.symptom)}</p>
      <p>${esc(b.detail)}</p>
      <p class="fix"><strong>Resolution.</strong> ${esc(b.resolution)}</p>
      <p class="test">${esc(b.test)}</p>
    </div>`).join("");
}
$("findings").innerHTML = html;

/* ----------------------------------------------------------------- timeline */
function timeline(sc) {
  const total = sc.total_ticks || 1;

  const rows = sc.nodes.map(node => {
    const bands = sc.leader_intervals.filter(i => i.node === node).map(i => {
      const live = i.end >= total ? " live" : "";
      return `<div class="band leader${live}" style="left:${pct(i.start,total)}%;width:${Math.max(pct(i.end-i.start,total),1)}%"
                title="${esc(node)} led term ${i.term}, ticks ${i.start}–${i.end}">T${i.term}</div>`;
    }).join("");
    return `<div class="tl-row"><div class="tl-name">${esc(node)}</div><div class="tl-track">${bands}</div></div>`;
  }).join("");

  const faults = sc.fault_windows.map(w => {
    const label = w.node ? `${w.kind} ${w.node}` : w.label;
    return `<div class="band fault" style="left:${pct(w.start,total)}%;width:${Math.max(pct(w.end-w.start,total),1)}%"
              title="${esc(label)}, ticks ${w.start}–${w.end}">${esc(label)}</div>`;
  }).join("");
  const pings = sc.fault_windows.map(w =>
    `<div class="inject" style="left:${pct(w.start,total)}%" title="fault injected at tick ${w.start}"></div>`).join("");

  const marks = sc.writes.map(w => {
    const at = w.acked_tick != null ? w.acked_tick : w.proposed_tick;
    const key = w.command && w.command.key ? w.command.key : "?";
    const title = `${key}=${w.command.value} · proposed tick ${w.proposed_tick}` +
      (w.acked_tick != null ? ` · acked tick ${w.acked_tick}` : ` · never acknowledged (${w.status})`);
    return `<div class="mark" style="left:${pct(at,total)}%" title="${esc(title)}">
              <div class="pip ${w.status === "acked" ? "acked" : ""}"></div><div class="cap">${esc(key)}</div></div>`;
  }).join("");

  return `<div class="tl">
    ${rows}
    <div class="tl-row"><div class="tl-name fault-name">faults</div><div class="tl-track">${faults}${pings}</div></div>
    <div class="tl-marks-row"><div></div><div class="tl-marks">${marks}</div></div>
    <div class="axis"><div></div><div class="t"><span>t0</span><span>t${total}</span></div></div>
    <div class="key">
      <span><i class="leader"></i> leadership</span>
      <span><i class="fault"></i> active fault</span>
      <span><i class="pip acked"></i> acknowledged</span>
      <span><i class="pip"></i> never acknowledged</span>
    </div>
  </div>`;
}

function writesTable(sc) {
  if (!sc.writes.length) return "";
  return `<div class="tbl-wrap"><table>
    <thead><tr><th>idx</th><th>term</th><th>command</th><th>proposed</th><th>acked</th><th>status</th></tr></thead>
    <tbody>${sc.writes.map(w => `<tr>
      <td>${w.index}</td><td>${w.term}</td>
      <td class="cmd">${esc(w.command.key)}=${esc(w.command.value)}</td>
      <td>${w.proposed_tick}</td>
      <td>${w.acked_tick != null ? w.acked_tick : "—"}</td>
      <td class="${w.status === "acked" ? "ok" : "no"}">${esc(w.status)}</td>
    </tr>`).join("")}</tbody></table></div>`;
}

$("scenarios").innerHTML = DATA.scenarios.map((sc, i) => {
  const v = sc.check.violations.length;
  const verdict = v === 0
    ? `<span class="verdict pass">pass</span>`
    : `<span class="verdict fail">${v} violation${v > 1 ? "s" : ""}</span>`;
  const violations = v === 0 ? "" : `<div class="panel finding" style="margin-top:1.1rem"><h3>Violations</h3>` +
    sc.check.violations.slice(0, 20).map(x =>
      `<p><code>${esc(x.kind)}</code> @ tick ${x.tick} — ${esc(x.message)}</p>`).join("") + `</div>`;

  return `<details class="panel scenario">
    <summary>
      <span class="caret">›</span>
      <span class="sum-id">${String(i + 1).padStart(2, "0")}</span>
      <span class="sum-main">
        <span class="sum-name">${esc(sc.name)}</span>
        <div class="sum-fault">${esc(sc.fault_summary)}</div>
      </span>
      ${verdict}
    </summary>
    <div class="body-wrap"><div class="body-inner"><div class="scenario-body">
      <p class="desc">${esc(sc.description)}</p>
      ${timeline(sc)}
      ${writesTable(sc)}
      <div class="counters">
        <span>seed <b>${sc.seed}</b></span>
        <span>ticks <b>${sc.total_ticks}</b></span>
        <span>sent <b>${sc.counters.messages_sent}</b></span>
        <span>delivered <b>${sc.counters.messages_delivered}</b></span>
        <span>dropped <b>${sc.counters.messages_dropped}</b></span>
        <span>delayed <b>${sc.counters.messages_delayed}</b></span>
        <span>acked <b>${sc.check.acked_writes}</b></span>
      </div>
      ${violations}
    </div></div></div>
  </details>`;
}).join("");

$("footer").innerHTML =
  `results by <code>run_chaos.py</code> · page by <code>build_site.py</code> · engine ` +
  `<a href="${esc(DATA.engine.repo)}">${esc(DATA.engine.repo.replace("https://github.com/",""))}</a> cloned read-only, not modified`;
</script>
</body>
</html>
"""


def main() -> int:
    if not RESULTS.exists():
        raise SystemExit(f"{RESULTS} not found -- run `python run_chaos.py` first.")
    data = json.loads(RESULTS.read_text(encoding="utf-8"))
    OUT.write_text(build_html(data), encoding="utf-8")
    size_kb = OUT.stat().st_size / 1024
    print(f"Wrote {OUT} ({size_kb:.1f} KB, data inlined)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
