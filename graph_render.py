"""
Render the NIA knowledge graph to a single self-contained HTML file.

No CDN, no build step, no server, no browser storage — inline CSS and inline JS
only, so it opens offline from a file:// URL and hosts free anywhere.

────────────────────────────────────────────────────────────────────────────
REWRITTEN 2026-08-18 — the first version became a hairball once the corpus was
real (2,500+ patents, 2,400+ signals, 400+ theses). Two changes fix that:

1. LEVEL OF DETAIL. The default view shows only ORGANISATIONS and
   TECHNOLOGIES — the map. Every patent, paper, grant, trial and posting
   collapses into the WEIGHT of an org->tech edge, so "Medtronic works on deep
   brain stimulation, 47 documents deep" is one line instead of 47 dots.
   Individual works appear only when you select something. A graph that shows
   everything at once shows nothing.

2. ANATOMICAL LAYOUT. Technologies sit where they actually act, on a sagittal
   brain: DBS at the basal ganglia, BCI at motor cortex, retinal prosthetics at
   the occipital pole, spinal cord stimulation at the cord. Organisations are
   placed near the technology they file most in.

   Position therefore carries information. Someone who knows neuroanatomy can
   find a field without reading labels; someone who doesn't learns the anatomy
   from the layout. This is the difference between a brain-shaped graph and a
   graph drawn on top of a brain picture — the second is decoration, and
   decoration on a data display is a cost, not a feature.

   The force-directed view is still available via a toggle, because anatomy is
   the wrong frame for questions about people and institutions.

VISUAL ENCODING (dataviz method; palette validated with
scripts/validate_palette.js --mode dark --pairs all):

  ORG     #3987e5  circle     categorical slot 1
  PERSON  #d95926  square     categorical slot 2
  TECH    #199e70  diamond    categorical slot 3
  WORK    neutral  small dot  folded to "Other"

Three categorical hues, not four: this is a node-link diagram, so any two types
can end up adjacent and the all-pairs pairlist applies. No four-hue set from the
palette clears the all-pairs normal-vision floor, and the method is explicit
that a normal-vision failure is not excusable by secondary encoding — past three
slots you fold to "Other". Folding WORK is also right on the merits: works are
connective tissue, orgs and technologies are what a reader scans for. Shape
encodes type redundantly with colour, so identity never depends on colour alone.

Usage:
    python graph_render.py                       # nia_graph.sqlite -> nia_graph.html
    python graph_render.py --db g.sqlite --out g.html --max-orgs 140
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

BG, CARD, BORDER = "#050810", "#0d1117", "#1f2937"
TEXT, DIM = "#e5e7eb", "#6b7280"
AMBER = "#f59e0b"

TYPE_STYLE = {
    "ORG":    {"color": "#3987e5", "shape": "circle",  "label": "Organisation"},
    "PERSON": {"color": "#d95926", "shape": "square",  "label": "Person"},
    "TECH":   {"color": "#199e70", "shape": "diamond", "label": "Technology"},
    "WORK":   {"color": "#8b93a7", "shape": "dot",     "label": "Work (patent, paper, grant, trial, posting…)"},
}


def _shape_svg(shape: str, color: str) -> str:
    if shape == "circle":
        return f'<circle cx="8" cy="8" r="6" fill="{color}"/>'
    if shape == "dot":
        return f'<circle cx="8" cy="8" r="3.4" fill="{color}"/>'
    if shape == "square":
        return f'<rect x="2.5" y="2.5" width="11" height="11" fill="{color}"/>'
    return f'<path d="M8 1.5 L14.5 8 L8 14.5 L1.5 8 Z" fill="{color}"/>'


def load(db: str, max_orgs: int, max_works: int, max_people: int):
    try:
        from neuro_taxonomy import TECH_ANATOMY
    except Exception:
        TECH_ANATOMY = {}

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    total_ents = cur.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_rels = cur.execute("SELECT COUNT(*) FROM relations").fetchone()[0]

    ents = {r["id"]: dict(r) for r in cur.execute(
        "SELECT id,name,type,subtype,description,source_doc,weight FROM entities")}
    rels = [dict(r) for r in cur.execute(
        "SELECT source_id,target_id,predicate,source_doc,weight FROM relations")]

    # ── Aggregate: ORG --(via WORK)--> TECH ─────────────────────────────────
    # This is the collapse that makes the graph readable. A patent is evidence
    # that an org works on a technology; it is not itself interesting at map
    # scale. So every work becomes +1 on an org->tech edge.
    work_orgs = defaultdict(set)
    work_techs = defaultdict(set)
    org_direct_tech = Counter()

    for r in rels:
        s, t, p = r["source_id"], r["target_id"], r["predicate"]
        st, tt = ents.get(s, {}).get("type"), ents.get(t, {}).get("type")
        if st == "WORK" and tt == "ORG" and p in ("filed_by", "funded_by", "sponsored_by"):
            work_orgs[s].add(t)
        elif st == "WORK" and tt == "TECH" and p == "about":
            work_techs[s].add(t)
        elif st == "ORG" and tt == "TECH" and p == "hiring_for":
            org_direct_tech[(s, t)] += 1

    org_tech = Counter()
    for w, orgs in work_orgs.items():
        for o in orgs:
            for tch in work_techs.get(w, ()):
                org_tech[(o, tch)] += 1

    org_weight = Counter()
    for (o, _t), n in org_tech.items():
        org_weight[o] += n
    for (o, _t), n in org_direct_tech.items():
        org_weight[o] += n

    top_orgs = [o for o, _ in org_weight.most_common(max_orgs)]
    org_set = set(top_orgs)

    techs = [e for e in ents.values() if e["type"] == "TECH"]
    techs.sort(key=lambda e: -(e["weight"] or 0))

    # ── Node arrays ─────────────────────────────────────────────────────────
    nodes, idx = [], {}

    def push(eid, extra=None):
        if eid in idx:
            return idx[eid]
        e = ents[eid]
        i = len(nodes)
        idx[eid] = i
        n = {"i": i, "n": e["name"][:90], "t": e["type"], "s": e["subtype"] or "",
             "d": (e["description"] or "")[:240], "p": e["source_doc"] or "",
             "w": float(e["weight"] or 1)}
        if extra:
            n.update(extra)
        nodes.append(n)
        return i

    for e in techs:
        a = TECH_ANATOMY.get(e["name"], {})
        push(e["id"], {"ax": a.get("x"), "ay": a.get("y"),
                       "region": a.get("region", ""), "depth": a.get("depth", "")})
    for oid in top_orgs:
        push(oid)

    # detail layer — hidden until something is selected
    work_ids = [w for w in work_orgs if work_orgs[w] & org_set]
    work_ids.sort(key=lambda w: -(ents.get(w, {}).get("weight") or 0))
    for w in work_ids[:max_works]:
        if w in ents:
            push(w)

    people = [e for e in ents.values() if e["type"] == "PERSON"]
    people.sort(key=lambda e: -(e["weight"] or 0))
    for e in people[:max_people]:
        push(e["id"])

    # ── Edges ───────────────────────────────────────────────────────────────
    agg = []
    for (o, t), n in org_tech.items():
        if o in idx and t in idx:
            agg.append({"a": idx[o], "b": idx[t], "w": n, "k": "works"})
    for (o, t), n in org_direct_tech.items():
        if o in idx and t in idx:
            agg.append({"a": idx[o], "b": idx[t], "w": n, "k": "hiring"})

    detail = []
    for r in rels:
        a, b = r["source_id"], r["target_id"]
        if a in idx and b in idx:
            ta, tb = ents[a]["type"], ents[b]["type"]
            if "WORK" in (ta, tb) or "PERSON" in (ta, tb):
                detail.append({"a": idx[a], "b": idx[b], "p": r["predicate"],
                               "d": r["source_doc"] or ""})

    aliases = defaultdict(list)
    for r in cur.execute("SELECT entity_id, alias FROM aliases"):
        if r["entity_id"] in idx:
            aliases[idx[r["entity_id"]]].append(r["alias"])
    merged = {k: v for k, v in aliases.items() if len(v) > 1}

    by_type = Counter(n["t"] for n in nodes)
    by_sub = Counter(n["s"] for n in nodes if n["t"] == "WORK" and n["s"])
    conn.close()

    stats = {"total_entities": total_ents, "total_relations": total_rels,
             "shown": len(nodes), "orgs_total": len(org_weight),
             "orgs_shown": len(top_orgs), "agg_edges": len(agg),
             "by_type": dict(by_type), "by_sub": dict(by_sub),
             "works_total": len(work_orgs), "works_shown": min(len(work_ids), max_works)}
    return nodes, agg, detail, merged, stats


def render(nodes, agg, detail, merged, stats, out, title):
    payload = json.dumps({"nodes": nodes, "agg": agg, "detail": detail,
                          "merged": merged, "style": TYPE_STYLE},
                         separators=(",", ":"))
    built = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    legend = "".join(
        f'<span class="lg"><svg width="16" height="16" viewBox="0 0 16 16">'
        f'{_shape_svg(s["shape"], s["color"])}</svg>{s["label"]}</span>'
        for t, s in TYPE_STYLE.items())
    sub_rows = "".join(f"<tr><td>{s}</td><td class='num'>{c}</td></tr>"
                       for s, c in sorted(stats["by_sub"].items(), key=lambda x: -x[1]))

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
*{{box-sizing:border-box}}
body{{margin:0;background:{BG};color:{TEXT};font:13px/1.5 'Courier New',ui-monospace,monospace}}
header{{padding:14px 20px;border-bottom:1px solid {BORDER};display:flex;
       align-items:baseline;gap:16px;flex-wrap:wrap}}
h1{{margin:0;font-size:15px;letter-spacing:.14em;color:{AMBER};text-transform:uppercase}}
.sub{{color:{DIM};font-size:11px}}
.wrap{{display:grid;grid-template-columns:1fr 350px;height:calc(100vh - 54px)}}
@media(max-width:920px){{.wrap{{grid-template-columns:1fr;height:auto}} #stage{{height:64vh}}}}
#stage{{position:relative;overflow:hidden}}
#cv{{display:block;width:100%;height:100%;cursor:grab}}
#cv:active{{cursor:grabbing}}
aside{{border-left:1px solid {BORDER};background:{CARD};overflow-y:auto;padding:14px}}
/* Not full-width: the scalp-level technologies (EEG) sit at the top centre of
   the brain, and a toolbar spanning the canvas hides their labels. */
.bar{{position:absolute;top:12px;left:12px;width:min(560px,62%);display:flex;
     gap:8px;flex-wrap:wrap;align-items:center;z-index:5}}
input[type=search],select{{background:rgba(13,17,23,.94);border:1px solid {BORDER};
  color:{TEXT};padding:6px 9px;font:12px 'Courier New',monospace;border-radius:4px}}
input[type=search]{{flex:1;min-width:140px}}
button{{background:rgba(13,17,23,.94);border:1px solid {BORDER};color:{TEXT};
  padding:6px 11px;font:12px 'Courier New',monospace;border-radius:4px;cursor:pointer}}
button:hover{{border-color:{AMBER};color:{AMBER}}}
button.on{{border-color:{AMBER};color:{AMBER}}}
.legend{{position:absolute;bottom:10px;left:12px;display:flex;gap:13px;flex-wrap:wrap;
  background:rgba(13,17,23,.93);border:1px solid {BORDER};padding:7px 11px;
  border-radius:6px;font-size:11px;z-index:5}}
.lg{{display:flex;align-items:center;gap:6px}}
.hint{{position:absolute;bottom:10px;right:12px;color:{DIM};font-size:10.5px;
  background:rgba(13,17,23,.93);border:1px solid {BORDER};padding:6px 10px;border-radius:6px}}
h2{{font-size:11px;letter-spacing:.12em;color:{AMBER};text-transform:uppercase;
   margin:16px 0 7px;border-bottom:1px solid {BORDER};padding-bottom:5px}}
h2:first-child{{margin-top:0}}
table{{width:100%;border-collapse:collapse;font-size:11.5px}}
td{{padding:3px 4px;border-bottom:1px solid #161d29;vertical-align:top}}
.num{{text-align:right;color:{AMBER};width:52px}}
#detail .nm{{font-size:14px;color:{AMBER};word-break:break-word}}
#detail .ty{{color:{DIM};font-size:11px;margin:2px 0 8px}}
.prov{{color:{DIM};font-size:10.5px;word-break:break-all;border-left:2px solid {BORDER};
  padding-left:7px;margin:5px 0}}
.rel{{display:flex;gap:6px;padding:3px 0;border-bottom:1px solid #161d29;font-size:11.5px}}
.rel .pd{{color:#9fb3c8;min-width:96px}}
.rel .tg{{flex:1;cursor:pointer}} .rel .tg:hover{{color:{AMBER};text-decoration:underline}}
.pill{{display:inline-block;background:#111827;border:1px solid {BORDER};border-radius:10px;
  padding:1px 7px;margin:2px 3px 2px 0;font-size:10.5px;color:#9fb3c8}}
#tip{{position:absolute;pointer-events:none;background:rgba(5,8,16,.97);border:1px solid {AMBER};
  border-radius:4px;padding:6px 9px;font-size:11.5px;max-width:280px;display:none;z-index:20}}
details summary{{cursor:pointer;color:{DIM};font-size:11px;margin-top:10px}}
.note{{color:{DIM};font-size:11px}}
</style></head><body>
<header>
  <h1>NIA · Neurotech Knowledge Graph</h1>
  <span class="sub">{stats['total_entities']:,} entities · {stats['total_relations']:,} relations ·
  showing {stats['orgs_shown']} of {stats['orgs_total']:,} organisations · built {built}</span>
</header>
<div class="wrap">
  <div id="stage">
    <div class="bar">
      <input type="search" id="q" placeholder="search organisation or technology…" autocomplete="off">
      <button id="mode" class="on">brain view</button>
      <button id="works">show works</button>
      <button id="fit">fit</button>
    </div>
    <canvas id="cv"></canvas>
    <div class="legend">{legend}</div>
    <div class="hint">click a node · drag to pan · scroll to zoom</div>
    <div id="tip"></div>
  </div>
  <aside>
    <div id="detail"></div>
    <h2>Work breakdown</h2>
    <table>{sub_rows}</table>
    <details><summary>Accessible table view</summary><table id="tbl"></table></details>
    <p class="note" style="margin-top:14px">Organisations are placed beside the
    technology they file in most. Technology position is anatomical — where the
    approach acts on the nervous system.</p>
  </aside>
</div>
<script>
const D={payload}, N=D.nodes, AGG=D.agg, DET=D.detail, S=D.style, MERGED=D.merged;
const TECH=N.filter(n=>n.t==='TECH'), ORG=N.filter(n=>n.t==='ORG');

const aggAdj=N.map(()=>[]); AGG.forEach((e,i)=>{{aggAdj[e.a].push(i);aggAdj[e.b].push(i);}});
const detAdj=N.map(()=>[]); DET.forEach((e,i)=>{{detAdj[e.a].push(i);detAdj[e.b].push(i);}});

const cv=document.getElementById('cv'), cx=cv.getContext('2d');
let W=0,H=0,DPR=Math.min(devicePixelRatio||1,2);
function size(){{const r=cv.parentElement.getBoundingClientRect();
  W=r.width;H=r.height||560;cv.width=W*DPR;cv.height=H*DPR;cx.setTransform(DPR,0,0,DPR,0,0);}}
size();

/* ── brain geometry: normalised sagittal outline, head facing left ───────── */
/* Sagittal outline, head facing LEFT. Enough points that the quadratic-through-
   midpoints smoothing follows the real silhouette instead of rounding it into a
   ball: bulging frontal pole, flattened vertex, occipital bump, and a temporal
   lobe that projects forward and down beneath a Sylvian notch. */
/* The brain is drawn as a few SEPARATE overlapping masses rather than one
   closed outline. A single path that tries to carve the Sylvian fissure and the
   temporal pole gets rounded away by smoothing and reads as a lumpy circle;
   overlapping ovoids with a shared fill read as a brain immediately, which is
   how textbook sagittal diagrams are drawn. */
const CEREBRUM=[
  [.085,.360],[.092,.286],[.122,.216],[.170,.156],[.234,.110],[.310,.078],
  [.396,.060],[.486,.056],[.572,.066],[.650,.092],[.716,.132],[.770,.186],
  [.810,.252],[.832,.324],[.836,.396],[.822,.462],[.792,.516],[.748,.556],
  [.694,.582],[.632,.596],[.564,.602],[.494,.600],[.424,.590],[.356,.572],
  [.290,.544],[.226,.506],[.168,.458],[.122,.410]];
/* Temporal lobe — projects forward and down, under the Sylvian fissure. */
const TEMPORAL=[
  [.300,.548],[.372,.578],[.446,.606],[.512,.626],[.560,.648],[.580,.684],
  [.566,.718],[.518,.738],[.456,.740],[.394,.724],[.340,.694],[.302,.652],
  [.288,.598]];
const CEREBELLUM=[
  [.664,.578],[.726,.574],[.782,.594],[.818,.630],[.826,.676],[.804,.716],
  [.756,.738],[.700,.734],[.660,.708],[.644,.664],[.646,.616]];
const STEM=[
  [.596,.596],[.644,.606],[.658,.684],[.666,.762],[.672,.848],[.678,.928],
  [.620,.928],[.614,.848],[.606,.762],[.592,.684]];

let layout='brain', showWorks=false, sel=null, query='', hov=null;
const P=N.map(()=>({{x:0,y:0,vx:0,vy:0,r:5}}));

function brainBox(){{
  const m=Math.min(W,H)*0.90, ox=(W-m)/2, oy=(H-m)/2+m*0.035;
  return {{m,ox,oy}};
}}
const BX=()=>brainBox();
const bx=(u)=>{{const b=BX();return b.ox+u*b.m;}};
const by=(v)=>{{const b=BX();return b.oy+v*b.m;}};

function layoutBrain(){{
  TECH.forEach(n=>{{
    const i=n.i;
    P[i].x = bx(n.ax!=null?n.ax:0.5);
    P[i].y = by(n.ay!=null?n.ay:0.4);
    P[i].r = 9+Math.min(Math.sqrt(n.w)*0.9,9);
  }});
  /* each org gravitates to its strongest technology, then de-overlaps */
  const best={{}};
  for(const e of AGG){{
    const o=N[e.a].t==='ORG'?e.a:e.b, t=N[e.a].t==='TECH'?e.a:e.b;
    if(N[o].t!=='ORG'||N[t].t!=='TECH') continue;
    if(!best[o]||e.w>best[o].w) best[o]={{t,w:e.w}};
  }}
  const around={{}};
  ORG.forEach(n=>{{
    const b=best[n.i];
    if(!b){{P[n.i].x=bx(0.02);P[n.i].y=by(0.05+Math.random()*0.9);P[n.i].r=4;return;}}
    around[b.t]=(around[b.t]||0)+1;
    const k=around[b.t], ring=1+Math.floor(k/9), ang=(k%9)/9*6.2832+ring*0.7;
    const rad=(26+ring*20);
    P[n.i].x=P[b.t].x+Math.cos(ang)*rad;
    P[n.i].y=P[b.t].y+Math.sin(ang)*rad*0.82;
    P[n.i].r=4.5+Math.min(Math.sqrt(b.w)*1.25,10);
  }});
  for(let s=0;s<90;s++) relax(false);
}}

function layoutForce(){{
  N.forEach((n,i)=>{{P[i].x=W/2+Math.cos(i*2.399)*Math.sqrt(i)*16;
    P[i].y=H/2+Math.sin(i*2.399)*Math.sqrt(i)*16;
    P[i].r=n.t==='TECH'?11:(n.t==='ORG'?5+Math.min(Math.sqrt(n.w),8):3);}});
  for(let s=0;s<260;s++) relax(true);
}}

function relax(withSprings){{
  const act=activeIdx();
  const cell=46, grid=new Map();
  for(const i of act){{const k=((P[i].x/cell)|0)+','+((P[i].y/cell)|0);
    if(!grid.has(k))grid.set(k,[]);grid.get(k).push(i);}}
  for(const i of act){{
    const gx=(P[i].x/cell)|0, gy=(P[i].y/cell)|0;
    for(let dx=-1;dx<=1;dx++)for(let dy=-1;dy<=1;dy++){{
      const b=grid.get((gx+dx)+','+(gy+dy)); if(!b)continue;
      for(const j of b){{ if(i===j)continue;
        let ddx=P[i].x-P[j].x, ddy=P[i].y-P[j].y;
        let d=Math.hypot(ddx,ddy); const need=P[i].r+P[j].r+5;
        if(d<0.01){{ddx=Math.random()-.5;ddy=Math.random()-.5;d=1;}}
        if(d<need){{const f=(need-d)/d*0.5;
          if(N[i].t!=='TECH'){{P[i].x+=ddx*f;P[i].y+=ddy*f;}}
          if(N[j].t!=='TECH'){{P[j].x-=ddx*f;P[j].y-=ddy*f;}}}}
      }}}}
  }}
  if(withSprings) for(const e of AGG){{
    const a=P[e.a],b=P[e.b],dx=b.x-a.x,dy=b.y-a.y,d=Math.hypot(dx,dy)||1;
    const f=(d-90)*0.012, ux=dx/d*f, uy=dy/d*f;
    a.x+=ux;a.y+=uy;b.x-=ux;b.y-=uy;
  }}
}}

function activeIdx(){{
  const out=[];
  N.forEach((n,i)=>{{ if(visible(i)) out.push(i); }});
  return out;
}}
function visible(i){{
  const n=N[i];
  if(n.t==='TECH') return true;
  if(n.t==='ORG')  return true;
  /* works & people only when expanded or when attached to the selection */
  if(showWorks) return true;
  if(sel===null) return false;
  return detAdj[i].some(ei=>DET[ei].a===sel||DET[ei].b===sel);
}}

let zoom=1,ox=0,oy=0;
function fit(){{
  const act=activeIdx(); if(!act.length)return;
  const xs=act.map(i=>P[i].x), ys=act.map(i=>P[i].y);
  const x0=Math.min(...xs),x1=Math.max(...xs),y0=Math.min(...ys),y1=Math.max(...ys);
  const w=x1-x0||1,h=y1-y0||1;
  zoom=Math.min(W/(w+110),H/(h+110),1.9);
  ox=W/2-((x0+x1)/2)*zoom; oy=H/2-((y0+y1)/2)*zoom;
}}
const sx=i=>P[i].x*zoom+ox, sy=i=>P[i].y*zoom+oy;

function smooth(pts,close){{
  cx.beginPath();
  const p=pts.map(([u,v])=>[bx(u)*zoom+ox, by(v)*zoom+oy]);
  cx.moveTo(p[0][0],p[0][1]);
  for(let i=0;i<p.length-1;i++){{
    const mx=(p[i][0]+p[i+1][0])/2, my=(p[i][1]+p[i+1][1])/2;
    cx.quadraticCurveTo(p[i][0],p[i][1],mx,my);
  }}
  if(close){{const mx=(p[p.length-1][0]+p[0][0])/2,my=(p[p.length-1][1]+p[0][1])/2;
    cx.quadraticCurveTo(p[p.length-1][0],p[p.length-1][1],mx,my);cx.closePath();}}
}}
function drawBrain(){{
  cx.save();
  cx.strokeStyle='#2e4260'; cx.lineWidth=1.6; cx.fillStyle='rgba(20,29,45,.92)';
  smooth(STEM,true); cx.fill(); cx.stroke();
  smooth(CEREBELLUM,true); cx.fill(); cx.stroke();
  smooth(TEMPORAL,true); cx.fill(); cx.stroke();
  smooth(CEREBRUM,true); cx.fill(); cx.stroke();
  /* a few sulci so it reads as a brain rather than a blob */
  cx.strokeStyle='#243консь'; cx.strokeStyle='#25344c'; cx.lineWidth=1.1;
  [[[.215,.190],[.290,.278],[.268,.372],[.312,.442]],
   [[.352,.116],[.392,.242],[.348,.340],[.388,.424]],
   [[.492,.090],[.516,.230],[.472,.334],[.510,.430]],
   [[.630,.122],[.640,.264],[.594,.366],[.630,.452]],
   [[.752,.198],[.736,.322],[.780,.416]],
   [[.316,.566],[.412,.606],[.502,.632],[.566,.652]]]
   .forEach(sq=>{{smooth(sq,false);cx.stroke();}});
  cx.restore();
}}

function draw(){{
  cx.clearRect(0,0,W,H);
  if(layout==='brain') drawBrain();

  /* Aggregate edges: thickness = number of underlying documents.
     At rest in brain view we draw NONE. With 100+ organisations the crossings
     become a web that buries both the anatomy and the clusters, and position
     ALREADY encodes the relationship — an organisation sits beside the
     technology it works in most. Edges answer a question, so they appear when
     one is asked: selection, search, or hover. */
  const restQuiet = (layout==='brain' && sel===null && !query && hov===null);
  for(const e of AGG){{
    if(restQuiet) break;
    const near = sel===null || e.a===sel || e.b===sel || e.a===hov || e.b===hov;
    cx.globalAlpha = near?0.62:0.10;
    cx.strokeStyle = e.k==='hiring' ? '#8a6d2f' : '#3d5674';
    cx.lineWidth = Math.max(0.6, Math.min(Math.log(e.w+1)*1.25, 6))*(near?1:0.6);
    cx.beginPath(); cx.moveTo(sx(e.a),sy(e.a)); cx.lineTo(sx(e.b),sy(e.b)); cx.stroke();
  }}
  /* detail edges only when something is expanded */
  if(showWorks||sel!==null){{
    cx.lineWidth=1;
    for(const e of DET){{
      if(!visible(e.a)||!visible(e.b))continue;
      const near = sel===null||e.a===sel||e.b===sel;
      cx.globalAlpha=near?0.4:0.06; cx.strokeStyle='#2a3purple';
      cx.strokeStyle=near?'#55657f':'#212b3a';
      cx.beginPath(); cx.moveTo(sx(e.a),sy(e.a)); cx.lineTo(sx(e.b),sy(e.b)); cx.stroke();
    }}
  }}
  cx.globalAlpha=1;

  const act=activeIdx();
  for(const i of act){{
    const n=N[i], dim = (query && !n.n.toLowerCase().includes(query)) ? 0.12
              : (sel!==null && !nearSel(i) ? 0.2 : 1);
    shape(sx(i),sy(i),P[i].r*Math.min(zoom,1.4),n.t,S[n.t].color,dim);
  }}
  labels(act);
}}
function nearSel(i){{
  if(i===sel)return true;
  return aggAdj[i].some(e=>AGG[e].a===sel||AGG[e].b===sel)
      || detAdj[i].some(e=>DET[e].a===sel||DET[e].b===sel);
}}
function shape(px,py,r,type,fill,alpha){{
  cx.globalAlpha=alpha; cx.fillStyle=fill; cx.beginPath();
  const s=S[type].shape;
  if(s==='circle'||s==='dot') cx.arc(px,py,s==='dot'?Math.max(2,r*0.6):r,0,6.2832);
  else if(s==='square') cx.rect(px-r*0.85,py-r*0.85,r*1.7,r*1.7);
  else {{cx.moveTo(px,py-r*1.2);cx.lineTo(px+r*1.1,py);cx.lineTo(px,py+r*1.2);
        cx.lineTo(px-r*1.1,py);cx.closePath();}}
  cx.fill(); cx.lineWidth=2; cx.strokeStyle='{BG}'; cx.stroke(); cx.globalAlpha=1;
}}
function labels(act){{
  const boxes=[]; const hit=(a,b)=>!(a.x1<b.x0||a.x0>b.x1||a.y1<b.y0||a.y0>b.y1);
  /* technologies are the map's legend — always labelled, drawn first */
  const order=act.filter(i=>N[i].t==='TECH')
    .concat(act.filter(i=>N[i].t!=='TECH').sort((a,b)=>N[b].w-N[a].w));
  let drawn=0;
  for(const i of order){{
    const isTech=N[i].t==='TECH';
    if(!isTech && drawn>=16) break;
    if(query && !N[i].n.toLowerCase().includes(query)) continue;
    if(sel!==null && !nearSel(i)) continue;
    cx.font=(isTech?'600 11.5px':'11px')+' "Courier New",monospace';
    const t=N[i].n.length>28?N[i].n.slice(0,27)+'…':N[i].n;
    const w=cx.measureText(t).width;
    const px=sx(i), py=sy(i)-P[i].r*Math.min(zoom,1.4)-6;
    const box={{x0:px-w/2-3,x1:px+w/2+3,y0:py-11,y1:py+3}};
    if(box.x0<2||box.x1>W-2||box.y0<2||box.y1>H-2) continue;
    if(boxes.some(b=>hit(box,b))) continue;
    boxes.push(box); if(!isTech) drawn++;
    cx.textAlign='center'; cx.lineWidth=3.5; cx.strokeStyle='{BG}'; cx.strokeText(t,px,py);
    cx.fillStyle=isTech?'#c9f2e0':'{TEXT}'; cx.fillText(t,px,py);
  }}
}}

/* ── interaction ─────────────────────────────────────────────────────────── */
function pick(mx,my){{let best=null,bd=1e9;
  for(const i of activeIdx()){{
    const d=Math.hypot(sx(i)-mx,sy(i)-my), r=P[i].r*Math.min(zoom,1.4)+7;
    if(d<r&&d<bd){{bd=d;best=i;}}}}
  return best;}}
const tip=document.getElementById('tip');
let drag=null;
cv.addEventListener('mousedown',e=>{{drag={{x:e.offsetX,y:e.offsetY,ox,oy,moved:false}};}});
addEventListener('mouseup',()=>{{drag=null;}});
cv.addEventListener('mousemove',e=>{{
  if(drag){{if(Math.abs(e.offsetX-drag.x)+Math.abs(e.offsetY-drag.y)>3)drag.moved=true;
    ox=drag.ox+(e.offsetX-drag.x);oy=drag.oy+(e.offsetY-drag.y);draw();return;}}
  const i=pick(e.offsetX,e.offsetY);
  if(i!==hov){{ hov=i; draw(); }}
  if(i===null){{tip.style.display='none';return;}}
  const n=N[i];
  let h='<b style="color:'+S[n.t].color+'">'+esc(n.n)+'</b>';
  if(n.t==='TECH'&&n.region) h+='<br><span style="color:{DIM}">acts at '+esc(n.region)+'</span>';
  else h+='<br><span style="color:{DIM}">'+n.t+(n.s?' · '+n.s:'')+'</span>';
  if(n.t==='ORG'){{const tot=aggAdj[i].reduce((s,e)=>s+AGG[e].w,0);
    h+='<br><span style="color:{DIM}">'+tot+' documents across '+aggAdj[i].length+' technologies</span>';}}
  tip.innerHTML=h; tip.style.display='block';
  tip.style.left=Math.min(e.offsetX+14,W-290)+'px'; tip.style.top=(e.offsetY+14)+'px';
}});
cv.addEventListener('click',e=>{{
  if(drag&&drag.moved)return;
  const i=pick(e.offsetX,e.offsetY);
  sel=(i===sel)?null:i; showDetail(sel); draw();
}});
cv.addEventListener('wheel',e=>{{e.preventDefault();
  const f=e.deltaY<0?1.12:0.89;
  ox=e.offsetX-(e.offsetX-ox)*f; oy=e.offsetY-(e.offsetY-oy)*f; zoom*=f; draw();
}},{{passive:false}});
document.getElementById('q').addEventListener('input',e=>{{query=e.target.value.toLowerCase().trim();draw();}});
document.getElementById('mode').addEventListener('click',e=>{{
  layout=layout==='brain'?'force':'brain';
  e.target.textContent=layout==='brain'?'brain view':'force view';
  e.target.classList.toggle('on',layout==='brain');
  layout==='brain'?layoutBrain():layoutForce(); fit(); draw();
}});
document.getElementById('works').addEventListener('click',e=>{{
  showWorks=!showWorks; e.target.classList.toggle('on',showWorks);
  e.target.textContent=showWorks?'hide works':'show works'; draw();
}});
document.getElementById('fit').addEventListener('click',()=>{{fit();draw();}});
addEventListener('resize',()=>{{size();layout==='brain'?layoutBrain():layoutForce();fit();draw();}});
function esc(s){{return (s||'').replace(/[&<>"]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}})[c]);}}

function showDetail(i){{
  const d=document.getElementById('detail');
  if(i===null){{
    d.innerHTML='<h2>How to read this</h2><p class="note">Each <b>technology</b> '+
    'sits where it acts on the nervous system. Lines run from an organisation to '+
    'the technologies it works in, and line thickness is how many documents back '+
    'that up — patents, grants, trials, clearances and postings collapsed into one '+
    'edge.<br><br>Click anything to expand the documents behind it, each with the '+
    'source it came from.</p>'; return;}}
  const n=N[i];
  let h='<h2>Selection</h2><div class="nm">'+esc(n.n)+'</div><div class="ty">'+
    S[n.t].label+(n.s?' · '+esc(n.s):'')+(n.t==='TECH'&&n.region?' · acts at '+esc(n.region):'')+
    '</div>';
  if(n.d) h+='<div style="margin-bottom:8px">'+esc(n.d)+'</div>';
  if(n.p) h+='<div class="prov">source: '+esc(n.p)+'</div>';
  if(MERGED[i]) h+='<div><b style="color:{AMBER};font-size:11px">MERGED FROM '+
    MERGED[i].length+' SPELLINGS</b><br>'+MERGED[i].map(a=>'<span class="pill">'+esc(a)+'</span>').join('')+'</div>';

  const ae=aggAdj[i].map(e=>AGG[e]).sort((a,b)=>b.w-a.w);
  if(ae.length){{
    h+='<h2>'+(n.t==='ORG'?'Works in':'Worked on by')+'</h2>';
    for(const e of ae.slice(0,18)){{
      const o=(e.a===i)?e.b:e.a;
      h+='<div class="rel"><span class="pd">'+(e.k==='hiring'?'hiring for':e.w+' docs')+
         '</span><span class="tg" data-i="'+o+'" style="color:'+S[N[o].t].color+'">'+
         esc(N[o].n)+'</span></div>';
    }}
  }}
  const de=detAdj[i].map(e=>DET[e]).slice(0,25);
  if(de.length){{
    h+='<h2>Documents</h2>';
    for(const e of de){{
      const o=(e.a===i)?e.b:e.a;
      h+='<div class="rel"><span class="pd">'+esc(e.p)+'</span><span class="tg" data-i="'+o+
         '" style="color:'+S[N[o].t].color+'">'+esc(N[o].n)+'</span></div>'+
         (e.d?'<div class="prov">'+esc(e.d)+'</div>':'');
    }}
  }}
  d.innerHTML=h;
  d.querySelectorAll('.tg').forEach(el=>el.addEventListener('click',()=>{{
    sel=+el.dataset.i; showDetail(sel); draw();}}));
}}
document.getElementById('tbl').innerHTML=
  '<tr><td><b>Entity</b></td><td><b>Type</b></td><td class="num"><b>Docs</b></td></tr>'+
  N.filter(n=>n.t==='ORG'||n.t==='TECH')
   .map(n=>'<tr><td>'+esc(n.n)+'</td><td>'+n.t+'</td><td class="num">'+
     aggAdj[n.i].reduce((s,e)=>s+AGG[e].w,0)+'</td></tr>').join('');

layoutBrain(); fit(); showDetail(null); draw();
</script></body></html>"""
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    return len(html)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="nia_graph.sqlite")
    ap.add_argument("--out", default="nia_graph.html")
    ap.add_argument("--max-orgs", type=int, default=140)
    ap.add_argument("--max-works", type=int, default=400)
    ap.add_argument("--max-people", type=int, default=150)
    ap.add_argument("--max-nodes", type=int, default=0, help="(deprecated, ignored)")
    ap.add_argument("--title", default="NIA · Neurotech Knowledge Graph")
    a = ap.parse_args()

    nodes, agg, detail, merged, stats = load(a.db, a.max_orgs, a.max_works, a.max_people)
    n = render(nodes, agg, detail, merged, stats, a.out, a.title)
    print(f"  rendered -> {a.out}  ({n/1024:.0f} KB, self-contained)")
    print(f"  map layer   {stats['by_type'].get('TECH',0)} technologies · "
          f"{stats['orgs_shown']}/{stats['orgs_total']} organisations · {stats['agg_edges']} edges")
    print(f"  detail layer {stats['works_shown']}/{stats['works_total']} works · "
          f"{stats['by_type'].get('PERSON',0)} people (hidden until selected)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
