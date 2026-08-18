"""
Render the NIA knowledge graph to a single self-contained HTML file.

No CDN, no build step, no server, no browser storage — inline CSS and inline
JS only, so it opens offline from a file:// URL and hosts free anywhere. Same
constraint build_snapshot.py works under.

VISUAL ENCODING (per the dataviz method, palette validated with
scripts/validate_palette.js --mode dark --pairs all):

  ORG     #3987e5  circle     categorical slot 1
  PERSON  #d95926  square     categorical slot 2
  TECH    #199e70  diamond    categorical slot 3
  WORK    neutral  small dot  folded to "Other"

Why WORK is gray rather than a fourth hue: this is a node-link diagram, so ANY
two types can end up adjacent — the all-pairs pairlist applies. No four-hue set
from the palette clears the all-pairs normal-vision floor of ΔE 15, and the
method is explicit that a normal-vision failure is not excusable by secondary
encoding: past three slots you fold to "Other" or facet. Folding WORK is also
the right call on the merits — WORK nodes are the most numerous and are
connective tissue; ORG, PERSON and TECH are what a reader is actually scanning
for. Shape encodes type redundantly with colour, so identity is never carried
by colour alone.

Usage:
    python graph_render.py                       # nia_graph.sqlite -> nia_graph.html
    python graph_render.py --db g.sqlite --out g.html --max-nodes 600
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
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
    """Legend glyph — same shape vocabulary the canvas uses, so the legend and
    the plot are readable as one system."""
    if shape == "circle":
        return f'<circle cx="8" cy="8" r="6" fill="{color}"/>'
    if shape == "dot":
        return f'<circle cx="8" cy="8" r="3.4" fill="{color}"/>'
    if shape == "square":
        return f'<rect x="2.5" y="2.5" width="11" height="11" fill="{color}"/>'
    return f'<path d="M8 1.5 L14.5 8 L8 14.5 L1.5 8 Z" fill="{color}"/>'


def load(db: str, max_nodes: int):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    total_ents = cur.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_rels = cur.execute("SELECT COUNT(*) FROM relations").fetchone()[0]

    rows = cur.execute(
        "SELECT id,name,type,subtype,description,source_doc,weight,meta "
        "FROM entities ORDER BY weight DESC LIMIT ?", (max_nodes,)
    ).fetchall()
    keep = {r["id"] for r in rows}

    nodes = []
    idx = {}
    for i, r in enumerate(rows):
        idx[r["id"]] = i
        nodes.append({
            "i": i,
            "n": r["name"][:90],
            "t": r["type"],
            "s": r["subtype"] or "",
            "d": (r["description"] or "")[:260],
            "p": r["source_doc"] or "",
            "w": float(r["weight"] or 1),
        })

    edges = []
    for r in cur.execute("SELECT source_id,target_id,predicate,source_doc,weight FROM relations"):
        a, b = r["source_id"], r["target_id"]
        if a in keep and b in keep:
            edges.append({"a": idx[a], "b": idx[b], "p": r["predicate"],
                          "d": r["source_doc"] or ""})

    aliases = {}
    for r in cur.execute("SELECT entity_id, alias FROM aliases"):
        if r["entity_id"] in keep:
            aliases.setdefault(idx[r["entity_id"]], []).append(r["alias"])
    merged = {k: v for k, v in aliases.items() if len(v) > 1}

    by_type = Counter(n["t"] for n in nodes)
    by_sub = Counter(n["s"] for n in nodes if n["t"] == "WORK" and n["s"])
    by_pred = Counter(e["p"] for e in edges)
    conn.close()

    return nodes, edges, merged, {
        "total_entities": total_ents, "total_relations": total_rels,
        "shown_nodes": len(nodes), "shown_edges": len(edges),
        "by_type": dict(by_type), "by_sub": dict(by_sub), "by_pred": dict(by_pred),
        "truncated": total_ents > len(nodes),
    }


def render(nodes, edges, merged, stats, out: str, title: str):
    payload = json.dumps({"nodes": nodes, "edges": edges, "merged": merged,
                          "style": TYPE_STYLE}, separators=(",", ":"))
    built = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    legend = "".join(
        f'<span class="lg"><svg width="16" height="16" viewBox="0 0 16 16">'
        f'{_shape_svg(s["shape"], s["color"])}</svg>{s["label"]}'
        f'<b>{stats["by_type"].get(t, 0)}</b></span>'
        for t, s in TYPE_STYLE.items()
    )
    pred_rows = "".join(
        f"<tr><td><code>{p}</code></td><td class='num'>{c}</td></tr>"
        for p, c in sorted(stats["by_pred"].items(), key=lambda x: -x[1])
    )
    sub_rows = "".join(
        f"<tr><td>{s}</td><td class='num'>{c}</td></tr>"
        for s, c in sorted(stats["by_sub"].items(), key=lambda x: -x[1])
    )
    trunc = ""
    if stats["truncated"]:
        trunc = (f'<p class="warn">Showing the {stats["shown_nodes"]} highest-degree '
                 f'of {stats["total_entities"]} entities. The full graph is in the '
                 f'SQLite file — this is a rendering cap, not the corpus size.</p>')

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
*{{box-sizing:border-box}}
body{{margin:0;background:{BG};color:{TEXT};
     font:13px/1.5 'Courier New',ui-monospace,monospace}}
header{{padding:16px 20px;border-bottom:1px solid {BORDER};display:flex;
       align-items:baseline;gap:16px;flex-wrap:wrap}}
h1{{margin:0;font-size:15px;letter-spacing:.14em;color:{AMBER};text-transform:uppercase}}
.sub{{color:{DIM};font-size:11px}}
.wrap{{display:grid;grid-template-columns:1fr 340px;gap:0;height:calc(100vh - 58px)}}
@media(max-width:900px){{.wrap{{grid-template-columns:1fr;height:auto}}
  #cv{{height:60vh}}}}
#stage{{position:relative;background:{BG};overflow:hidden}}
#cv{{display:block;width:100%;height:100%;cursor:grab}}
#cv:active{{cursor:grabbing}}
aside{{border-left:1px solid {BORDER};background:{CARD};overflow-y:auto;padding:14px}}
.bar{{position:absolute;top:12px;left:12px;right:12px;display:flex;gap:8px;
     flex-wrap:wrap;align-items:center;z-index:5}}
input[type=search],select{{background:{CARD};border:1px solid {BORDER};color:{TEXT};
  padding:6px 9px;font:12px 'Courier New',monospace;border-radius:4px}}
input[type=search]{{flex:1;min-width:150px}}
button{{background:{CARD};border:1px solid {BORDER};color:{TEXT};padding:6px 11px;
  font:12px 'Courier New',monospace;border-radius:4px;cursor:pointer}}
button:hover{{border-color:{AMBER};color:{AMBER}}}
.legend{{position:absolute;bottom:12px;left:12px;display:flex;gap:14px;
  flex-wrap:wrap;background:rgba(13,17,23,.92);border:1px solid {BORDER};
  padding:8px 12px;border-radius:6px;font-size:11px;z-index:5}}
.lg{{display:flex;align-items:center;gap:6px;color:{TEXT}}}
.lg b{{color:{DIM};font-weight:400}}
h2{{font-size:11px;letter-spacing:.12em;color:{AMBER};text-transform:uppercase;
   margin:18px 0 8px;border-bottom:1px solid {BORDER};padding-bottom:5px}}
h2:first-child{{margin-top:0}}
table{{width:100%;border-collapse:collapse;font-size:11.5px}}
td{{padding:3px 4px;border-bottom:1px solid #161d29;vertical-align:top}}
.num{{text-align:right;color:{AMBER};width:52px}}
code{{color:#9fb3c8;font-size:11px}}
#detail .nm{{font-size:14px;color:{AMBER};margin-bottom:2px;word-break:break-word}}
#detail .ty{{color:{DIM};font-size:11px;margin-bottom:8px}}
#detail .ds{{color:{TEXT};margin-bottom:8px;font-size:12px}}
.prov{{color:{DIM};font-size:10.5px;word-break:break-all;
  border-left:2px solid {BORDER};padding-left:7px;margin:6px 0}}
.rel{{display:flex;gap:6px;padding:3px 0;border-bottom:1px solid #161d29;font-size:11.5px}}
.rel .pd{{color:#9fb3c8;min-width:104px}}
.rel .tg{{flex:1;cursor:pointer}}
.rel .tg:hover{{color:{AMBER};text-decoration:underline}}
.warn{{color:{AMBER};font-size:11px;border:1px solid {BORDER};padding:7px;
  border-radius:4px;background:#150f00}}
.hint{{color:{DIM};font-size:11.5px}}
.pill{{display:inline-block;background:#111827;border:1px solid {BORDER};
  border-radius:10px;padding:1px 7px;margin:2px 3px 2px 0;font-size:10.5px;color:#9fb3c8}}
#tip{{position:absolute;pointer-events:none;background:rgba(5,8,16,.97);
  border:1px solid {AMBER};border-radius:4px;padding:6px 9px;font-size:11.5px;
  max-width:290px;display:none;z-index:20}}
details summary{{cursor:pointer;color:{DIM};font-size:11px;margin-top:10px}}
</style></head><body>

<header>
  <h1>NIA · Neurotech Knowledge Graph</h1>
  <span class="sub">{stats['total_entities']} entities · {stats['total_relations']} relations · built {built}</span>
</header>

<div class="wrap">
  <div id="stage">
    <div class="bar">
      <input type="search" id="q" placeholder="search entity…" autocomplete="off">
      <select id="ft">
        <option value="">all types</option>
        <option value="ORG">organisations</option>
        <option value="PERSON">people</option>
        <option value="TECH">technologies</option>
        <option value="WORK">works</option>
      </select>
      <button id="re">relayout</button>
      <button id="fit">fit</button>
    </div>
    <canvas id="cv"></canvas>
    <div class="legend">{legend}</div>
    <div id="tip"></div>
  </div>

  <aside>
    <div id="detail">
      <h2>Selection</h2>
      <p class="hint">Click any node to see its connections and the source
      document each connection came from. Drag to pan, scroll to zoom.</p>
    </div>
    {trunc}
    <h2>Relation types</h2>
    <table>{pred_rows}</table>
    <h2>Work breakdown</h2>
    <table>{sub_rows}</table>
    <details>
      <summary>Accessible table view (all shown nodes)</summary>
      <table id="tbl"></table>
    </details>
  </aside>
</div>

<script>
const DATA = {payload};
const S = DATA.style, N = DATA.nodes, E = DATA.edges, MERGED = DATA.merged;

/* adjacency */
const adj = N.map(()=>[]);
E.forEach((e,i)=>{{ adj[e.a].push(i); adj[e.b].push(i); }});

/* ── force layout ───────────────────────────────────────────────────────── */
const cv = document.getElementById('cv'), cx = cv.getContext('2d');
let W=0,H=0,DPR=Math.min(devicePixelRatio||1,2);
function size(){{
  const r = cv.parentElement.getBoundingClientRect();
  W=r.width; H=r.height||520;
  cv.width=W*DPR; cv.height=H*DPR; cx.setTransform(DPR,0,0,DPR,0,0);
}}
size(); addEventListener('resize',()=>{{size();draw();}});

const P = N.map((n,i)=>({{
  x: W/2 + Math.cos(i*2.399)*Math.sqrt(i)*15,
  y: H/2 + Math.sin(i*2.399)*Math.sqrt(i)*15,
  vx:0, vy:0, r: Math.min(4+Math.sqrt(n.w)*1.7, 17)
}}));

function layout(steps){{
  for(let s=0;s<steps;s++){{
    const k = 1 - s/steps;
    /* repulsion — coarse grid keeps it O(n·neighbours) not O(n²) */
    const cell=70, grid=new Map();
    P.forEach((p,i)=>{{
      const key=((p.x/cell)|0)+','+((p.y/cell)|0);
      if(!grid.has(key)) grid.set(key,[]); grid.get(key).push(i);
    }});
    P.forEach((p,i)=>{{
      const gx=(p.x/cell)|0, gy=(p.y/cell)|0;
      for(let dx=-1;dx<=1;dx++) for(let dy=-1;dy<=1;dy++){{
        const b=grid.get((gx+dx)+','+(gy+dy)); if(!b) continue;
        for(const j of b){{ if(i===j) continue;
          const q=P[j]; let ddx=p.x-q.x, ddy=p.y-q.y;
          let d2=ddx*ddx+ddy*ddy; if(d2<0.01){{ddx=Math.random()-.5;ddy=Math.random()-.5;d2=1;}}
          if(d2>26000) continue;
          const f=520/d2; p.vx+=ddx*f*k; p.vy+=ddy*f*k;
        }}
      }}
    }});
    /* springs */
    for(const e of E){{
      const a=P[e.a], b=P[e.b];
      const dx=b.x-a.x, dy=b.y-a.y, d=Math.hypot(dx,dy)||1;
      const f=(d-72)*0.015*k;
      const ux=dx/d*f, uy=dy/d*f;
      a.vx+=ux; a.vy+=uy; b.vx-=ux; b.vy-=uy;
    }}
    /* gravity + integrate */
    P.forEach(p=>{{
      p.vx += (W/2-p.x)*0.0022*k; p.vy += (H/2-p.y)*0.0022*k;
      p.x += (p.vx*=0.82); p.y += (p.vy*=0.82);
    }});
  }}
}}
layout(420);

/* ── view transform ─────────────────────────────────────────────────────── */
let zoom=1, ox=0, oy=0;
function fit(){{
  const xs=P.map(p=>p.x), ys=P.map(p=>p.y);
  const x0=Math.min(...xs), x1=Math.max(...xs), y0=Math.min(...ys), y1=Math.max(...ys);
  const w=x1-x0||1, h=y1-y0||1;
  zoom=Math.min(W/(w+120), H/(h+120), 2.4);
  ox=W/2-((x0+x1)/2)*zoom; oy=H/2-((y0+y1)/2)*zoom;
}}
fit();
const sx=p=>p.x*zoom+ox, sy=p=>p.y*zoom+oy;

let sel=null, hov=null, filter='', query='';

function visible(i){{
  const n=N[i];
  if(filter && n.t!==filter) return false;
  if(query && !n.n.toLowerCase().includes(query)) return false;
  return true;
}}
function inNeighbourhood(i){{
  if(sel===null) return true;
  if(i===sel) return true;
  return adj[sel].some(ei=>E[ei].a===i||E[ei].b===i);
}}

function shape(px,py,r,type,fill,alpha){{
  cx.globalAlpha=alpha; cx.fillStyle=fill;
  cx.beginPath();
  const s=S[type].shape;
  if(s==='circle'||s==='dot'){{ cx.arc(px,py,s==='dot'?Math.max(2.2,r*0.62):r,0,6.2832); }}
  else if(s==='square'){{ cx.rect(px-r*0.86,py-r*0.86,r*1.72,r*1.72); }}
  else {{ cx.moveTo(px,py-r*1.15); cx.lineTo(px+r*1.05,py);
          cx.lineTo(px,py+r*1.15); cx.lineTo(px-r*1.05,py); cx.closePath(); }}
  cx.fill();
  /* 2px surface ring so overlapping marks stay separable */
  cx.globalAlpha=alpha; cx.lineWidth=2; cx.strokeStyle='{BG}'; cx.stroke();
  cx.globalAlpha=1;
}}

function draw(){{
  cx.clearRect(0,0,W,H);
  /* edges first, recessive */
  cx.lineWidth=1;
  for(const e of E){{
    const va=visible(e.a), vb=visible(e.b);
    if(!va&&!vb) continue;
    const near = sel===null || e.a===sel || e.b===sel;
    cx.globalAlpha = near ? 0.5 : 0.07;
    cx.strokeStyle = near ? '#5b6b82' : '#232c3a';
    cx.beginPath();
    cx.moveTo(sx(P[e.a]),sy(P[e.a])); cx.lineTo(sx(P[e.b]),sy(P[e.b]));
    cx.stroke();
  }}
  cx.globalAlpha=1;
  /* nodes */
  N.forEach((n,i)=>{{
    const vis=visible(i), near=inNeighbourhood(i);
    const a = !vis ? 0.06 : (near ? 1 : 0.16);
    shape(sx(P[i]),sy(P[i]),P[i].r*Math.min(zoom,1.5),n.t,S[n.t].color,a);
  }});
  /* labels — selective, never one per node */
  cx.font='11px "Courier New",monospace'; cx.textAlign='center';
  const cands = N.map((n,i)=>i)
    .filter(i=>visible(i)&&inNeighbourhood(i))
    .sort((a,b)=>N[b].w-N[a].w).slice(0, sel!==null?26:22);
  /* Greedy collision avoidance: highest-degree nodes claim their label box
     first, and any later label whose box would overlap is dropped rather than
     drawn on top. Selective labelling beats a label on every node. */
  const boxes=[];
  const hit=(a,b)=>!(a.x1<b.x0||a.x0>b.x1||a.y1<b.y0||a.y0>b.y1);
  let drawn=0;
  for(const i of cands){{
    if(drawn >= (sel!==null?14:11)) break;
    const t=N[i].n.length>26?N[i].n.slice(0,25)+'…':N[i].n;
    const w=cx.measureText(t).width;
    const px=sx(P[i]), py=sy(P[i])-P[i].r*Math.min(zoom,1.5)-5;
    const box={{x0:px-w/2-3, x1:px+w/2+3, y0:py-10, y1:py+3}};
    if(box.x0<0||box.x1>W||box.y0<0||box.y1>H) continue;
    if(boxes.some(b=>hit(box,b))) continue;
    boxes.push(box); drawn++;
    cx.lineWidth=3; cx.strokeStyle='{BG}'; cx.strokeText(t,px,py);
    cx.fillStyle='{TEXT}'; cx.fillText(t,px,py);
  }}
}}
draw();

/* ── interaction ────────────────────────────────────────────────────────── */
function pick(mx,my){{
  let best=null,bd=1e9;
  N.forEach((n,i)=>{{
    if(!visible(i)) return;
    const d=Math.hypot(sx(P[i])-mx, sy(P[i])-my);
    const r=P[i].r*Math.min(zoom,1.5)+7;
    if(d<r&&d<bd){{bd=d;best=i;}}
  }});
  return best;
}}
const tip=document.getElementById('tip');
let drag=null;
cv.addEventListener('mousedown',e=>{{drag={{x:e.offsetX,y:e.offsetY,ox,oy,moved:false}};}});
addEventListener('mouseup',()=>{{drag=null;}});
cv.addEventListener('mousemove',e=>{{
  if(drag){{
    if(Math.abs(e.offsetX-drag.x)+Math.abs(e.offsetY-drag.y)>3) drag.moved=true;
    ox=drag.ox+(e.offsetX-drag.x); oy=drag.oy+(e.offsetY-drag.y); draw(); return;
  }}
  const i=pick(e.offsetX,e.offsetY);
  hov=i;
  if(i===null){{ tip.style.display='none'; return; }}
  const n=N[i];
  tip.innerHTML='<b style="color:'+S[n.t].color+'">'+esc(n.n)+'</b><br>'+
    '<span style="color:{DIM}">'+n.t+(n.s?' · '+n.s:'')+' · '+adj[i].length+' links</span>'+
    (n.d?'<br>'+esc(n.d.slice(0,150)):'');
  tip.style.display='block';
  tip.style.left=Math.min(e.offsetX+14, W-300)+'px';
  tip.style.top=(e.offsetY+14)+'px';
}});
cv.addEventListener('click',e=>{{
  if(drag&&drag.moved) return;
  const i=pick(e.offsetX,e.offsetY);
  sel = (i===sel)?null:i; showDetail(sel); draw();
}});
cv.addEventListener('wheel',e=>{{
  e.preventDefault();
  const f=e.deltaY<0?1.12:0.89;
  ox=e.offsetX-(e.offsetX-ox)*f; oy=e.offsetY-(e.offsetY-oy)*f;
  zoom*=f; draw();
}},{{passive:false}});

document.getElementById('q').addEventListener('input',e=>{{
  query=e.target.value.toLowerCase().trim(); draw();
}});
document.getElementById('ft').addEventListener('change',e=>{{
  filter=e.target.value; draw();
}});
document.getElementById('re').addEventListener('click',()=>{{layout(220);fit();draw();}});
document.getElementById('fit').addEventListener('click',()=>{{fit();draw();}});

function esc(s){{return (s||'').replace(/[&<>"]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}})[c]);}}

function showDetail(i){{
  const d=document.getElementById('detail');
  if(i===null){{
    d.innerHTML='<h2>Selection</h2><p class="hint">Click any node to see its '+
      'connections and the source document each connection came from. '+
      'Drag to pan, scroll to zoom.</p>'; return;
  }}
  const n=N[i];
  let h='<h2>Selection</h2><div class="nm">'+esc(n.n)+'</div>'+
        '<div class="ty">'+S[n.t].label+(n.s?' · '+esc(n.s):'')+
        ' · '+adj[i].length+' connections</div>';
  if(n.d) h+='<div class="ds">'+esc(n.d)+'</div>';
  if(n.p) h+='<div class="prov">source: '+esc(n.p)+'</div>';
  if(MERGED[i]) h+='<div><b style="color:{AMBER};font-size:11px">'+
    'MERGED FROM '+MERGED[i].length+' SPELLINGS</b><br>'+
    MERGED[i].map(a=>'<span class="pill">'+esc(a)+'</span>').join('')+'</div>';
  h+='<h2>Connections</h2>';
  const seen=new Set();
  for(const ei of adj[i]){{
    const e=E[ei], o=(e.a===i)?e.b:e.a;
    const key=e.p+':'+o; if(seen.has(key)) continue; seen.add(key);
    h+='<div class="rel"><span class="pd">'+esc(e.p)+'</span>'+
       '<span class="tg" data-i="'+o+'" style="color:'+S[N[o].t].color+'">'+
       esc(N[o].n)+'</span></div>'+
       (e.d?'<div class="prov">'+esc(e.d)+'</div>':'');
  }}
  d.innerHTML=h;
  d.querySelectorAll('.tg').forEach(el=>el.addEventListener('click',()=>{{
    sel=+el.dataset.i; showDetail(sel); draw();
  }}));
}}

/* accessible table view — identity never depends on the canvas */
document.getElementById('tbl').innerHTML =
  '<tr><td><b>Entity</b></td><td><b>Type</b></td><td class="num"><b>Links</b></td></tr>'+
  N.map((n,i)=>'<tr><td>'+esc(n.n)+'</td><td>'+n.t+(n.s?'/'+n.s:'')+
    '</td><td class="num">'+adj[i].length+'</td></tr>').join('');
</script>
</body></html>"""

    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    return len(html)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="nia_graph.sqlite")
    ap.add_argument("--out", default="nia_graph.html")
    ap.add_argument("--max-nodes", type=int, default=650)
    ap.add_argument("--title", default="NIA · Neurotech Knowledge Graph")
    a = ap.parse_args()

    nodes, edges, merged, stats = load(a.db, a.max_nodes)
    n = render(nodes, edges, merged, stats, a.out, a.title)

    print(f"  rendered -> {a.out}  ({n/1024:.0f} KB, self-contained)")
    print(f"  nodes {stats['shown_nodes']}/{stats['total_entities']}"
          f"   edges {stats['shown_edges']}/{stats['total_relations']}")
    print(f"  by type   {stats['by_type']}")
    if stats["truncated"]:
        print(f"  NOTE: rendering capped at {a.max_nodes} highest-degree nodes "
              f"(full graph remains in {a.db})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
