"""
NIA scoring — how much does a thing matter, and to whom.

Ranking neurotech entities by raw document count always returns Samsung,
because Samsung files more patents in a quarter than Paradromics will file
ever. Volume measures size, not significance. This module replaces it.

────────────────────────────────────────────────────────────────────────────
THE DESIGN, AND WHY IT IS TWO NUMBERS AND NOT ONE

The brief had three requirements that a single scalar cannot satisfy at once:

  1. no bias toward new OR old information
  2. objective across the whole domain, AND conditional on a query
  3. promote established science AND enable "cowboy science" — pushing the
     frontier, connecting non-obvious dots

Requirement 3 is the one that forces the shape. Established and frontier are
not two ends of one ruler; they are independent properties. Deep brain
stimulation at Medtronic is maximally established and minimally surprising.
An optogenetics-plus-focused-ultrasound thesis with one author is the reverse.
Collapsing both into one number necessarily buries one of them — and it is
always the frontier that gets buried, because consensus accumulates evidence
and novelty does not. So there are two orthogonal axes:

    ESTABLISHMENT  how thoroughly independent evidence corroborates this
    FRONTIER       how structurally unusual and unexploited this is

You can sort by either, or read the plane: high/high is a field breaking out,
low/high is where the cowboy science lives, high/low is the incumbent core,
low/low is noise.

────────────────────────────────────────────────────────────────────────────
AGE NEUTRALITY

This is the hard requirement, and it is where most bibliometrics fail. Raw
citation counts favour old work, which has had longer to accumulate. Recency
weighting favours new work. h-index favours long careers. Journal impact
factor measures the venue, not the work, over an arbitrary two-year window.

Two mechanisms here, together:

  * COHORT PERCENTILE. Every raw score is converted to a percentile against
    entities whose first evidence appeared in the same period. A 2016 entity is
    compared only with 2016 entities. "Top 5% for its cohort" then means the
    same thing in every era, by construction. This is the load-bearing one.

  * RATE, NOT VOLUME, for the time-sensitive component. Momentum is measured
    as recent activity relative to the entity's OWN historical rate, so an
    entity from 2012 that is accelerating today scores as well as one born
    yesterday.

There is a real cost, stated plainly: cohort percentiles are relative, so they
cannot tell you that a whole cohort was weak. They answer "who stood out among
their contemporaries", which is the age-neutral question. Absolute raw scores
are also returned, unnormalised, for anyone who wants the other question.

────────────────────────────────────────────────────────────────────────────
COMPONENTS

ESTABLISHMENT = f(corroboration, source independence, momentum)

  corroboration   IDF-weighted count of distinct EVIDENCE LAYERS attesting to
                  the entity — patent, thesis, preprint, grant, trial,
                  clearance, filing, posting. Weights are derived from the data
                  (log(N/N_layer)), not hand-set, so a rare layer like an FDA
                  clearance counts for more than another patent automatically,
                  and the weights re-derive themselves as the corpus grows.
                  Breadth, not volume: five patents is one layer.

  independence    distinct SOURCE SYSTEMS behind the evidence (EPO, NIH,
                  ClinicalTrials, openFDA, arXiv, SEC, ATS boards). One
                  organisation filing a hundred patents is one source saying
                  one thing a hundred times.

  momentum        recent rate ÷ own historical rate. >1 means accelerating.

FRONTIER = f(atypicality, brokerage, stage gap)

  atypicality     technology pairings that co-occur here but rarely co-occur in
                  the corpus, by pointwise mutual information. Grounded in Uzzi
                  et al. (Science, 2013): the highest-impact work pairs a
                  conventional core with an unusual tail combination. So this
                  measures the TAIL (10th-percentile PMI), not the median —
                  rewarding one strange juxtaposition rather than uniform
                  weirdness, which is mostly noise.

  brokerage       Burt's effective size: does this entity connect neighbours
                  who are otherwise unconnected? Bridging structural holes is
                  the network signature of non-obvious combination.

  stage gap       early-layer evidence (thesis, preprint) with no downstream
                  corroboration (trial, clearance). Unexploited, not failed.

RELEVANCE(query) = personalised PageRank (random walk with restart) from the
                  entities matching the query. Graph-native relevance, so a
                  result can be relevant by CONNECTION rather than by sharing
                  words with the query — which is the entire point of holding
                  the corpus as a graph.

Usage:
    python metrics.py --top 25
    python metrics.py --stance frontier --top 25
    python metrics.py --query "speech decoding" --stance balanced
    python metrics.py --explain "Medtronic"
    python metrics.py --selftest        # proves age-neutrality
"""
from __future__ import annotations

import argparse
import math
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

EARLY_LAYERS = {"thesis", "preprint", "paper", "article"}
LATE_LAYERS = {"trial", "clearance", "filing"}


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _year(datestr: str | None) -> int | None:
    if not datestr:
        return None
    m = re.match(r"(\d{4})", str(datestr))
    if not m:
        return None
    y = int(m.group(1))
    return y if 1900 < y < 2200 else None


def _source_system(source_doc: str | None) -> str:
    """Which upstream system this evidence came from."""
    s = (source_doc or "").lower()
    for key, name in (("epo", "epo"), ("patentsview", "uspto"), ("bigquery", "bigquery"),
                      ("nih", "nih"), ("clinicaltrials", "ctgov"), ("fda", "fda"),
                      ("arxiv", "arxiv"), ("biorxiv", "biorxiv"), ("medrxiv", "biorxiv"),
                      ("sec_edgar", "sec"), ("jobs:", "ats"), ("rss:", "press"),
                      ("thesis", "thesis"), ("patent:", "patent")):
        if key in s:
            return name
    return "other"


def percentile_rank(values: dict, keys) -> dict:
    """Percentile of each key's value among `keys`. Ties share the mean rank."""
    items = sorted(((values.get(k, 0.0), k) for k in keys))
    n = len(items)
    out = {}
    if n == 0:
        return out
    if n == 1:
        return {items[0][1]: 50.0}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and items[j + 1][0] == items[i][0]:
            j += 1
        mean_rank = (i + j) / 2.0
        pct = 100.0 * mean_rank / (n - 1)
        for k in range(i, j + 1):
            out[items[k][1]] = pct
        i = j + 1
    return out


# ─────────────────────────────────────────────────────────────────────────────
# core
# ─────────────────────────────────────────────────────────────────────────────

class Scorer:
    def __init__(self, conn: sqlite3.Connection, now_year: int | None = None):
        self.conn = conn
        conn.row_factory = sqlite3.Row
        self.now_year = now_year or datetime.now(timezone.utc).year
        self._load()

    def _load(self):
        cur = self.conn.cursor()
        self.ent = {}
        has_date = any(r["name"] == "event_date" for r in
                       cur.execute("PRAGMA table_info(entities)"))
        info = list(cur.execute("PRAGMA table_info(entities)"))
        has_qual = any(r["name"] == "quality" for r in info)
        has_val = any(r["name"] == "valence" for r in info)
        cols = ("id,name,type,subtype,source_doc,weight"
                + (",event_date" if has_date else "")
                + (",quality" if has_qual else "")
                + (",valence" if has_val else ""))
        for r in cur.execute(f"SELECT {cols} FROM entities"):
            d = dict(r)
            d.setdefault("event_date", None)
            d.setdefault("quality", 1.0)
            d.setdefault("valence", None)
            self.ent[d["id"]] = d

        self.rels = [dict(r) for r in cur.execute(
            "SELECT source_id,target_id,predicate,source_doc FROM relations")]

        self.nbr = defaultdict(set)
        for r in self.rels:
            a, b = r["source_id"], r["target_id"]
            if a in self.ent and b in self.ent:
                self.nbr[a].add(b)
                self.nbr[b].add(a)

    # ── evidence attached to each scorable entity ───────────────────────────
    def _evidence(self):
        """For every ORG and TECH: the works attesting to it."""
        ev = defaultdict(list)
        for r in self.rels:
            a, b = r["source_id"], r["target_id"]
            ea, eb = self.ent.get(a), self.ent.get(b)
            if not ea or not eb:
                continue
            if ea["type"] == "WORK" and eb["type"] in ("ORG", "TECH"):
                ev[b].append(ea)
            elif eb["type"] == "WORK" and ea["type"] in ("ORG", "TECH"):
                ev[a].append(eb)
            elif ea["type"] == "ORG" and eb["type"] == "TECH":
                ev[a].append(eb)   # direct org->tech (hiring)
        return ev

    def compute(self) -> dict:
        ev = self._evidence()
        targets = [e for e in self.ent.values() if e["type"] in ("ORG", "TECH")]

        # ── layer IDF, derived from the corpus rather than hand-set ─────────
        layer_ents = Counter()
        for eid, works in ev.items():
            for L in {w.get("subtype") or "work" for w in works}:
                layer_ents[L] += 1
        N = max(len(ev), 1)
        idf = {L: math.log(1 + N / max(c, 1)) for L, c in layer_ents.items()}

        # ── technology co-occurrence, for atypicality ───────────────────────
        tech_of_work = defaultdict(set)
        for r in self.rels:
            a, b = r["source_id"], r["target_id"]
            ea, eb = self.ent.get(a), self.ent.get(b)
            if not ea or not eb:
                continue
            if ea["type"] == "WORK" and eb["type"] == "TECH":
                tech_of_work[a].add(eb["name"])
            elif eb["type"] == "WORK" and ea["type"] == "TECH":
                tech_of_work[b].add(ea["name"])
        tcount, pcount, npairs = Counter(), Counter(), 0
        for w, ts in tech_of_work.items():
            for t in ts:
                tcount[t] += 1
            ts_l = sorted(ts)
            for i in range(len(ts_l)):
                for j in range(i + 1, len(ts_l)):
                    pcount[(ts_l[i], ts_l[j])] += 1
                    npairs += 1
        total_w = max(len(tech_of_work), 1)

        def pmi(t1, t2):
            a, b = sorted((t1, t2))
            joint = pcount.get((a, b), 0)
            if joint == 0:
                return -6.0          # co-occurs nowhere else: maximally atypical
            p_j = joint / max(npairs, 1)
            p_a = tcount[a] / total_w
            p_b = tcount[b] / total_w
            if p_a <= 0 or p_b <= 0:
                return 0.0
            return math.log(p_j / (p_a * p_b))

        raw = {}
        cohort = {}
        for e in targets:
            eid = e["id"]
            works = ev.get(eid, [])
            years = [y for y in (_year(w.get("event_date")) for w in works) if y]
            first = min(years) if years else _year(e.get("event_date")) or self.now_year
            cohort[eid] = first

            # Corroboration — breadth of layers, IDF weighted, each layer
            # scaled by the mean evidential quality of the documents in it.
            # A company whose only "coverage" layer is three press releases
            # should not get the same credit as one covered by a trial write-up.
            layers = {w.get("subtype") or "work" for w in works}
            corro = 0.0
            for L in layers:
                qs = [float(w.get("quality") or 1.0)
                      for w in works if (w.get("subtype") or "work") == L]
                corro += idf.get(L, 1.0) * (sum(qs) / len(qs) if qs else 1.0)

            # independence — distinct upstream systems
            systems = {_source_system(w.get("source_doc")) for w in works}
            indep = math.log(1 + len(systems))

            # momentum — recent rate vs own historical rate
            span = max(self.now_year - first + 1, 1)
            recent = sum(1 for y in years if y >= self.now_year - 1)
            hist_rate = len(years) / span if years else 0.0
            momentum = (recent / 2.0) / hist_rate if hist_rate > 0 else 0.0

            # atypicality — the TAIL of this entity's technology pairings
            techs = sorted({t for w in works for t in tech_of_work.get(w["id"], set())}
                           | ({e["name"]} if e["type"] == "TECH" else set()))
            pmis = []
            for i in range(len(techs)):
                for j in range(i + 1, len(techs)):
                    pmis.append(pmi(techs[i], techs[j]))
            if pmis:
                pmis.sort()
                tail = pmis[max(0, int(0.10 * len(pmis)) - 1)] if len(pmis) > 3 else pmis[0]
                atypical = max(0.0, -tail)
            else:
                atypical = 0.0

            # brokerage — Burt effective size over the entity's neighbourhood
            nb = self.nbr.get(eid, set())
            n = len(nb)
            if n > 1:
                ties = sum(1 for x in nb for y in nb if x < y and y in self.nbr.get(x, ()))
                eff = n - (2.0 * ties / n)
            else:
                eff = float(n)
            brokerage = math.log(1 + max(eff, 0.0))

            # stage gap — early evidence, nothing downstream yet
            gap = 1.0 if (layers & EARLY_LAYERS) and not (layers & LATE_LAYERS) else 0.0

            est_raw = corro * math.sqrt(1 + indep) * (0.6 + min(momentum, 3.0))
            fro_raw = (0.4 + atypical) * (0.5 + brokerage) * (1.0 + 0.6 * gap)

            # Valence is reported ALONGSIDE the two axes, never inside them.
            # A recall still corroborates that a company is real and shipping —
            # bad news is still evidence — so folding affect into establishment
            # would double-count the event and hide its direction. Only records
            # where affect was actually extracted contribute; never-extracted
            # (None) is not the same as extracted-and-neutral (0.0).
            vals = [float(w["valence"]) for w in works
                    if w.get("valence") is not None]
            valence = round(sum(vals) / len(vals), 3) if vals else None

            raw[eid] = {
                "name": e["name"], "type": e["type"], "cohort": first,
                "valence": valence, "n_valenced": len(vals),
                "n_works": len(works), "layers": sorted(layers),
                "systems": sorted(systems), "corroboration": round(corro, 3),
                "independence": round(indep, 3), "momentum": round(momentum, 3),
                "atypicality": round(atypical, 3), "brokerage": round(brokerage, 3),
                "stage_gap": gap,
                "establishment_raw": round(est_raw, 4),
                "frontier_raw": round(fro_raw, 4),
            }

        # ── age neutrality: percentile WITHIN cohort ────────────────────────
        # Cohort is (year, type), not year alone. An organisation and a
        # technology have structurally different evidence profiles — a
        # technology accumulates works from every company that touches it —
        # so ranking Medtronic against "Deep Brain Stimulation" produces a
        # percentile that means nothing, and it was making the same quality
        # tier score anywhere from 33 to 100 depending on how many technologies
        # happened to share its year.
        by_cohort = defaultdict(list)
        for eid, r in raw.items():
            by_cohort[(r["cohort"], r["type"])].append(eid)

        est_v = {k: v["establishment_raw"] for k, v in raw.items()}
        fro_v = {k: v["frontier_raw"] for k, v in raw.items()}
        for _c, ids in by_cohort.items():
            # A cohort of one has no distribution to rank against; fall back to
            # the global percentile so a lone entity is not handed a free 50.
            if len(ids) < 4:
                continue
            for eid, p in percentile_rank(est_v, ids).items():
                raw[eid]["establishment"] = round(p, 1)
            for eid, p in percentile_rank(fro_v, ids).items():
                raw[eid]["frontier"] = round(p, 1)
        # Fallback for thin cohorts: still rank like against like.
        for typ in ("ORG", "TECH"):
            ids = [k for k, v in raw.items() if v["type"] == typ]
            g_est = percentile_rank(est_v, ids)
            g_fro = percentile_rank(fro_v, ids)
            for eid in ids:
                raw[eid].setdefault("establishment", round(g_est.get(eid, 50.0), 1))
                raw[eid].setdefault("frontier", round(g_fro.get(eid, 50.0), 1))
        return raw

    # ── query-conditional relevance ─────────────────────────────────────────
    def relevance(self, query: str, alpha: float = 0.25, iters: int = 40) -> dict:
        """
        Personalised PageRank from the entities matching `query`.

        Walks the graph rather than matching text, so something can rank highly
        because of what it is CONNECTED to — a company that never uses your
        query's words but sits one hop from three things that do.
        """
        q = [t for t in re.split(r"\W+", (query or "").lower()) if len(t) > 2]
        seeds = [eid for eid, e in self.ent.items()
                 if q and all(t in (e["name"] or "").lower() for t in q)]
        if not seeds:
            seeds = [eid for eid, e in self.ent.items()
                     if any(t in (e["name"] or "").lower() for t in q)]
        if not seeds:
            return {}
        s = {eid: 1.0 / len(seeds) for eid in seeds}
        r = dict(s)
        for _ in range(iters):
            nxt = defaultdict(float)
            for eid, val in r.items():
                nb = self.nbr.get(eid, ())
                if not nb:
                    nxt[eid] += val
                    continue
                share = val * (1 - alpha) / len(nb)
                for n in nb:
                    nxt[n] += share
            for eid, val in s.items():
                nxt[eid] += alpha * val
            r = nxt
        mx = max(r.values()) if r else 1.0
        return {k: 100.0 * v / mx for k, v in r.items()}, seeds


def rank(scores: dict, stance: str = "balanced", rel: dict | None = None,
         etype: str | None = None, top: int = 25):
    rel = rel or {}
    out = []
    for eid, r in scores.items():
        if etype and r["type"] != etype:
            continue
        e, f = r["establishment"], r["frontier"]
        if stance == "established":
            base = e
        elif stance == "frontier":
            base = f
        else:
            # geometric mean: something must be decent on BOTH axes to lead a
            # balanced ranking. An arithmetic mean lets a 100/0 outlier tie a
            # genuinely dual 70/70, which is not what "balanced" should mean.
            base = math.sqrt(max(e, 0.1) * max(f, 0.1))
        if rel:
            base *= (0.25 + 0.75 * (rel.get(eid, 0.0) / 100.0))
        out.append((base, eid, r))
    out.sort(key=lambda x: -x[0])
    return out[:top]


# ─────────────────────────────────────────────────────────────────────────────
# self-test — the age-neutrality claim has to be checkable
# ─────────────────────────────────────────────────────────────────────────────

def selftest() -> int:
    import os
    import tempfile
    path = os.path.join(tempfile.gettempdir(), "nia_metric_selftest.sqlite")
    if os.path.exists(path):
        os.remove(path)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from graph_build import GraphBuilder

    gb = GraphBuilder(path)
    TECHS = ["Deep Brain Stimulation", "Brain-Computer Interface", "EEG & Seizure",
             "Optogenetics", "Retinal & Visual", "Focused Ultrasound"]

    # Each cohort year gets FOUR organisations at four quality tiers, identical
    # across years. An age-neutral metric must (a) rank the tiers correctly
    # inside every cohort and (b) give the same tier the same score regardless
    # of year. Testing only identical entities would pass trivially.
    TIERS = {
        "strong": [("patent", "epo"), ("patent", "epo"), ("grant", "nih"),
                   ("trial", "clinicaltrials"), ("clearance", "fda"),
                   ("filing", "sec_edgar")],
        "medium": [("patent", "epo"), ("grant", "nih"), ("trial", "clinicaltrials")],
        "weak":   [("patent", "epo"), ("patent", "epo")],
        "thin":   [("preprint", "arxiv")],
    }
    for k, yr in enumerate(range(2012, 2024)):
        for tier, spec in TIERS.items():
            org = f"{tier.title()} {yr}"
            for i, (sub, src) in enumerate(spec):
                w = gb.add_entity("WORK", f"{org}-{sub}-{i}", subtype=sub,
                                  description=f"{sub} for {org}",
                                  source_doc=f"{src}:{org}:{i}",
                                  event_date=f"{yr}-06-15")
                gb.add_relation(w, gb.add_entity("ORG", org, source_doc=f"{src}:{org}"),
                                "filed_by", f"{src}:{org}:{i}")
                gb.add_relation(w, gb.add_entity("TECH", TECHS[(k + i) % len(TECHS)]),
                                "about", f"{src}:{org}:{i}")
    gb.finalise(); gb.close()

    conn = sqlite3.connect(path)
    sc = Scorer(conn, now_year=2026).compute()
    rows = [(r["cohort"], r["establishment"], r["name"].split()[0], r["name"])
            for r in sc.values()
            if r["name"].split()[0] in ("Strong", "Medium", "Weak", "Thin")]
    rows.sort()

    print("  (a) does the ranking INSIDE each cohort track quality?")
    bad_order = 0
    by_year = defaultdict(dict)
    for c, e, tier, _n in rows:
        by_year[c][tier] = e
    for c in sorted(by_year):
        t = by_year[c]
        ok = t.get("Strong", 0) >= t.get("Medium", 0) >= t.get("Weak", 0)
        bad_order += 0 if ok else 1
    print(f"      cohorts where strong >= medium >= weak: "
          f"{len(by_year)-bad_order}/{len(by_year)}")

    print("\n  (b) does the SAME tier score the same across years?")
    for tier in ("Strong", "Medium", "Weak", "Thin"):
        vals = [e for _c, e, t, _n in rows if t == tier]
        if vals:
            print(f"      {tier:<7} min={min(vals):5.1f}  max={max(vals):5.1f}  "
                  f"spread={max(vals)-min(vals):5.1f}")

    ys = [c for c, _e, _t, _n in rows]
    es = [e for _c, e, _t, _n in rows]
    my, me = sum(ys) / len(ys), sum(es) / len(es)
    num = sum((y - my) * (e - me) for y, e in zip(ys, es))
    den = math.sqrt(sum((y - my) ** 2 for y in ys) * sum((e - me) ** 2 for e in es))
    corr = num / den if den else 0.0
    print(f"\n  (c) correlation(year, establishment) = {corr:+.3f}  "
          f"(0 = perfectly age-neutral)")
    ok = abs(corr) < 0.25 and bad_order == 0
    print("\n  PASS — quality ordered correctly, and age explains none of it"
          if ok else "\n  FAIL")
    conn.close()
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="nia_graph.sqlite")
    ap.add_argument("--stance", choices=["established", "frontier", "balanced"],
                    default="balanced")
    ap.add_argument("--query", default="")
    ap.add_argument("--type", choices=["ORG", "TECH"], default=None)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--explain", default="")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()

    if a.selftest:
        return selftest()

    conn = sqlite3.connect(a.db)
    sc = Scorer(conn)
    scores = sc.compute()

    if a.explain:
        hit = [r for r in scores.values() if a.explain.lower() in r["name"].lower()]
        if not hit:
            print(f"  no entity matching {a.explain!r}")
            return 1
        r = sorted(hit, key=lambda x: -x["n_works"])[0]
        print(f"\n  {r['name']}   [{r['type']}]   cohort {r['cohort']}\n")
        print(f"    ESTABLISHMENT  {r['establishment']:5.1f}  (percentile within cohort)")
        print(f"      corroboration {r['corroboration']:6.2f}   layers: {', '.join(r['layers'])}")
        print(f"      independence  {r['independence']:6.2f}   systems: {', '.join(r['systems'])}")
        print(f"      momentum      {r['momentum']:6.2f}   (>1 = accelerating)")
        print(f"\n    FRONTIER       {r['frontier']:5.1f}  (percentile within cohort)")
        print(f"      atypicality   {r['atypicality']:6.2f}   (unusual technology pairing)")
        print(f"      brokerage     {r['brokerage']:6.2f}   (bridges unconnected neighbours)")
        print(f"      stage gap     {r['stage_gap']:6.2f}   (early evidence, nothing downstream)")
        if r.get("valence") is not None:
            direction = ("positive" if r["valence"] > 0.25 else
                         "negative" if r["valence"] < -0.25 else "mixed/neutral")
            print(f"\n    SENTIMENT     {r['valence']:+5.2f}  {direction}  "
                  f"(across {r['n_valenced']} records with extracted affect)")
        else:
            print("\n    SENTIMENT        --  no affect extracted yet "
                  "(run: python main.py affect)")
        print(f"\n    {r['n_works']} works")
        return 0

    rel, seeds = ({}, [])
    if a.query:
        got = sc.relevance(a.query)
        if got:
            rel, seeds = got
            print(f"\n  query {a.query!r} matched {len(seeds)} seed entities")
        else:
            print(f"\n  query {a.query!r} matched nothing — showing unconditional ranking")

    rows = rank(scores, a.stance, rel, a.type, a.top)
    print(f"\n  stance={a.stance}" + (f"  query={a.query!r}" if a.query else "") + "\n")
    print(f"  {'':>4} {'ESTAB':>6} {'FRONT':>6} {'REL':>5}  {'COH':>5}  ENTITY")
    print("  " + "-" * 74)
    for i, (_s, eid, r) in enumerate(rows, 1):
        rv = f"{rel.get(eid,0):5.0f}" if rel else "    -"
        print(f"  {i:>3}. {r['establishment']:6.1f} {r['frontier']:6.1f} {rv}  "
              f"{r['cohort']:>5}  {r['name'][:46]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
