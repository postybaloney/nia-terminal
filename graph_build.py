"""
NIA knowledge graph builder.

Collapses the whole corpus — patents, theses, grants, trials, FDA actions,
preprints, newsletter coverage and job postings — into ONE graph in which the
layers are connected, and writes it to a single SQLite file.

────────────────────────────────────────────────────────────────────────────
WHY THIS SHAPE

Modelled on Glitch-Cat-Club/graph-memory-starter. Three tables, deterministic
IDs, one recursive query. Its central claim — "spend intelligence at build;
answer from structure" — is the right trade here: do the expensive resolution
work once at build time so query time is a structural lookup rather than a
reasoning chain over documents.

Three ideas taken directly from it:

  1. THREE TABLES ONLY — entities, relations, aliases. Anything more is
     unreadable by a human, and a knowledge graph nobody can read is a
     liability rather than an asset.

  2. uuid5(type + normalised name) AS THE PRIMARY KEY — identity is derived,
     not assigned. Re-running the build is idempotent, and the same
     organisation arriving from EPO, NIH and a job board collapses into one
     node automatically. This is the single most important property here:
     without it "Boston Scientific Corp", "BOSTON SCIENTIFIC CORPORATION" and
     "Boston Scientific Neuromodulation" are three unrelated nodes and the
     cross-layer story the graph exists to tell simply does not appear.

  3. source_doc ON EVERY ROW — every node and every edge knows where it came
     from. For an intelligence product this is not bookkeeping, it is the
     product: any claim can be traced back to a filing, grant or posting.

ONTOLOGY (deliberately tiny — see neuro_taxonomy.py)
  Nodes      ORG · PERSON · TECH · WORK
  Edges      filed_by · authored_by · affiliated_with · about · funded_by
             sponsored_by · hiring_for · advances_to · cites

USAGE
  python graph_build.py                 # live DB  -> nia_graph.sqlite
  python graph_build.py --demo          # synthetic corpus, no DB needed
  python graph_build.py --out g.sqlite
  python graph_build.py --explain "Boston Scientific"
  python graph_build.py --path "Neuralink" "Motor Cortex"
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

log = logging.getLogger("graph")

# Stable namespace so IDs are reproducible across machines and runs.
NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS entities (
    id          TEXT PRIMARY KEY,   -- uuid5(type + normalised name)
    name        TEXT NOT NULL,      -- display name (best observed spelling)
    type        TEXT NOT NULL,      -- ORG | PERSON | TECH | WORK
    subtype     TEXT,               -- WORK: patent|paper|thesis|trial|grant|...
    description TEXT,
    source_doc  TEXT,               -- provenance: where we first saw it
    weight      REAL DEFAULT 1.0,   -- degree-derived, filled at finalise()
    first_seen  TEXT,
    meta        TEXT                -- JSON blob (url, amount, dates...)
);

CREATE TABLE IF NOT EXISTS relations (
    source_id  TEXT NOT NULL,
    target_id  TEXT NOT NULL,
    predicate  TEXT NOT NULL,
    source_doc TEXT,
    weight     REAL DEFAULT 1.0,
    PRIMARY KEY (source_id, target_id, predicate)
);

CREATE TABLE IF NOT EXISTS aliases (
    entity_id TEXT NOT NULL,
    alias     TEXT NOT NULL,
    PRIMARY KEY (entity_id, alias)
);

CREATE INDEX IF NOT EXISTS ix_ent_type    ON entities(type);
CREATE INDEX IF NOT EXISTS ix_rel_source  ON relations(source_id);
CREATE INDEX IF NOT EXISTS ix_rel_target  ON relations(target_id);
CREATE INDEX IF NOT EXISTS ix_rel_pred    ON relations(predicate);
CREATE INDEX IF NOT EXISTS ix_alias_alias ON aliases(alias);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Entity resolution
# ─────────────────────────────────────────────────────────────────────────────

_LEGAL_SUFFIXES = (
    "incorporated", "inc", "llc", "l l c", "ltd", "limited", "corp",
    "corporation", "company", "co", "gmbh", "ag", "kg", "kgaa", "ab", "as",
    "a s", "sa", "s a", "nv", "n v", "bv", "b v", "plc", "lp", "llp", "pte",
    "pty", "oy", "aps", "spa", "s p a", "srl", "sarl", "holdings", "holding",
    "group", "technologies", "technology", "labs", "laboratories",
    "the", "of", "and",
)

# Known parents — subsidiary spellings that should collapse onto one node.
# Kept SHORT and explicit on purpose: a long fuzzy-matching table silently
# merges things that are genuinely different, which is worse than missing a
# merge. Extend only when you have seen the variant in real data.
_CANONICAL: dict[str, str] = {
    "boston scientific neuromodulation": "boston scientific",
    "boston scientific scimed": "boston scientific",
    "medtronic xomed": "medtronic",
    "medtronic bakken research center": "medtronic",
    "medtronic puerto rico operations": "medtronic",
    "abbott medical": "abbott",
    "abbott laboratories": "abbott",
    "st jude medical": "abbott",
    "advanced neuromodulation systems": "abbott",
    "cochlear bone anchored solutions": "cochlear",
    "livanova usa": "livanova",
    "cyberonics": "livanova",
    "neuropace": "neuropace",
    "regents of the university of california": "university of california",
    "the regents of the university of california": "university of california",
    "leland stanford junior university": "stanford university",
    "board of trustees of the leland stanford junior university": "stanford university",
    "massachusetts institute of technology": "mit",
    "president and fellows of harvard college": "harvard university",
    "johns hopkins university": "johns hopkins",
    "case western reserve university": "case western reserve",
}


def normalise_org(name: str) -> str:
    """
    Reduce an organisation name to a comparison key.

    This is the function that decides whether the graph tells a connected story
    or shows the same company six times. Conservative by design: strip legal
    form and punctuation, then apply an explicit canonical map. No fuzzy
    matching — a wrong merge is harder to notice than a missed one.
    """
    s = (name or "").lower().strip()
    s = re.sub(r"[.,''`\"()]", " ", s)
    s = re.sub(r"[-/&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = [t for t in s.split() if t]
    while tokens and tokens[-1] in _LEGAL_SUFFIXES:
        tokens.pop()
    while tokens and tokens[0] in ("the",):
        tokens.pop(0)
    s = " ".join(tokens).strip()

    return _CANONICAL.get(s, s)


def normalise_person(name: str) -> str:
    """
    People are messier than orgs: 'SMITH, JOHN A' / 'John A. Smith' / 'J Smith'.
    Normalise to 'first_initial lastname', which merges the common variants
    without collapsing genuinely different people too aggressively.
    """
    s = (name or "").strip()
    if not s:
        return ""
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if "," in (name or ""):                       # "SMITH, JOHN A"
        last, _, first = name.partition(",")
        parts = [first.strip(), last.strip()]
    else:
        toks = s.split()
        parts = [" ".join(toks[:-1]), toks[-1]] if len(toks) > 1 else ["", s]

    first, last = parts[0].lower().strip(), parts[1].lower().strip()
    fi = first[0] if first else ""
    return f"{fi} {last}".strip()


def entity_id(etype: str, key: str) -> str:
    return str(uuid.uuid5(NS, f"{etype}:{key}"))


# ─────────────────────────────────────────────────────────────────────────────
# Builder
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class GraphStats:
    entities: int = 0
    relations: int = 0
    aliases: int = 0
    by_type: dict | None = None
    by_predicate: dict | None = None
    merged_orgs: int = 0


class GraphBuilder:
    def __init__(self, path: str = "nia_graph.sqlite"):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.executescript(SCHEMA)
        self._ent_cache: dict[str, str] = {}
        self._names_seen: dict[str, set] = {}

    # ── writing ──────────────────────────────────────────────────────────────

    def add_entity(
        self, etype: str, name: str, *, subtype: str = "",
        description: str = "", source_doc: str = "", meta: str = "",
    ) -> str | None:
        name = (name or "").strip()
        if not name or len(name) < 2:
            return None

        if etype == "ORG":
            key = normalise_org(name)
        elif etype == "PERSON":
            key = normalise_person(name)
        else:
            key = re.sub(r"\s+", " ", name.lower()).strip()
        if not key:
            return None

        eid = entity_id(etype, key)

        # Track spelling variants so the alias table earns its place.
        self._names_seen.setdefault(eid, set()).add(name)

        if eid not in self._ent_cache:
            self.conn.execute(
                "INSERT OR IGNORE INTO entities "
                "(id,name,type,subtype,description,source_doc,first_seen,meta) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (eid, name, etype, subtype, description[:900], source_doc,
                 datetime.now(timezone.utc).isoformat(), meta),
            )
            self._ent_cache[eid] = name
        return eid

    def add_relation(self, src: str | None, tgt: str | None,
                     predicate: str, source_doc: str = "") -> None:
        if not src or not tgt or src == tgt:
            return
        self.conn.execute(
            "INSERT INTO relations (source_id,target_id,predicate,source_doc,weight) "
            "VALUES (?,?,?,?,1.0) "
            "ON CONFLICT(source_id,target_id,predicate) "
            "DO UPDATE SET weight = weight + 1.0",
            (src, tgt, predicate, source_doc),
        )

    def finalise(self) -> GraphStats:
        """Write aliases, compute degree weights, drop orphan TECH nodes."""
        for eid, names in self._names_seen.items():
            for n in names:
                self.conn.execute(
                    "INSERT OR IGNORE INTO aliases (entity_id, alias) VALUES (?,?)",
                    (eid, n),
                )
        merged = sum(1 for v in self._names_seen.values() if len(v) > 1)

        self.conn.execute("""
            UPDATE entities SET weight = 1.0 + (
                SELECT COUNT(*) FROM relations r
                WHERE r.source_id = entities.id OR r.target_id = entities.id
            )
        """)
        # A concept nobody references is noise on the canvas.
        self.conn.execute("""
            DELETE FROM entities WHERE type='TECH' AND id NOT IN (
                SELECT target_id FROM relations UNION SELECT source_id FROM relations
            )
        """)
        self.conn.commit()

        cur = self.conn.cursor()
        ents = cur.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        rels = cur.execute("SELECT COUNT(*) FROM relations").fetchone()[0]
        als = cur.execute("SELECT COUNT(*) FROM aliases").fetchone()[0]
        by_type = dict(cur.execute(
            "SELECT type, COUNT(*) FROM entities GROUP BY type").fetchall())
        by_pred = dict(cur.execute(
            "SELECT predicate, COUNT(*) FROM relations GROUP BY predicate").fetchall())

        return GraphStats(ents, rels, als, by_type, by_pred, merged)

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Ingest from the live Postgres corpus
# ─────────────────────────────────────────────────────────────────────────────

def build_from_db(gb: GraphBuilder, days: int = 3650) -> None:
    """Read the operational DB and project it into the graph."""
    from db import get_session
    from db.models import RawPatent
    from neuro_taxonomy import classify_tech

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    with get_session() as session:
        # ── Patents ──────────────────────────────────────────────────────────
        patents = session.query(RawPatent).all()
        log.info("graph: %d patents", len(patents))
        for p in patents:
            wid = gb.add_entity(
                "WORK", f"{p.source_id}", subtype="patent",
                description=(p.title or "")[:900],
                source_doc=f"patent:{p.source}:{p.source_id}",
                meta=(p.title or "")[:200],
            )
            for a in (p.assignees or []):
                oid = gb.add_entity("ORG", a.get("name", ""),
                                    source_doc=f"patent:{p.source_id}")
                gb.add_relation(wid, oid, "filed_by", f"patent:{p.source_id}")
            for inv in (p.inventors or [])[:10]:
                pid = gb.add_entity("PERSON", inv.get("name", ""),
                                    source_doc=f"patent:{p.source_id}")
                gb.add_relation(wid, pid, "authored_by", f"patent:{p.source_id}")
                # Inventor -> assignee is the affiliation backbone that lets
                # people-level paths cross into organisations.
                for a in (p.assignees or [])[:2]:
                    oid = gb.add_entity("ORG", a.get("name", ""))
                    gb.add_relation(pid, oid, "affiliated_with", f"patent:{p.source_id}")
            for concept in classify_tech(p.title, p.abstract):
                tid = gb.add_entity("TECH", concept)
                gb.add_relation(wid, tid, "about", f"patent:{p.source_id}")

        # ── Signals (grants, trials, FDA, articles, preprints, jobs, filings) ─
        try:
            from db.signal_models import Signal
            signals = session.query(Signal).all()
            log.info("graph: %d signals", len(signals))
            for s in signals:
                sub = {
                    "grant": "grant", "trial": "trial", "clearance": "clearance",
                    "approval": "clearance", "article": "article",
                    "preprint": "preprint", "posting": "posting", "filing": "filing",
                }.get(s.signal_type or "", "work")

                wid = gb.add_entity(
                    "WORK", f"{s.source}:{s.source_id}", subtype=sub,
                    description=(s.title or "")[:900],
                    source_doc=f"{s.source}:{s.source_id}",
                    meta=(s.url or "")[:300],
                )
                if s.organization:
                    oid = gb.add_entity("ORG", s.organization,
                                        source_doc=f"{s.source}:{s.source_id}")
                    pred = {
                        "grant": "funded_by", "trial": "sponsored_by",
                        "posting": "filed_by",
                    }.get(s.signal_type or "", "filed_by")
                    gb.add_relation(wid, oid, pred, f"{s.source}:{s.source_id}")

                    # A job posting is a capability signal about the ORG, not
                    # just a document — connect the org straight to the concept.
                    if s.signal_type == "posting":
                        for t in (s.tags or []):
                            if t and t != "strategic-hire":
                                tid = gb.add_entity("TECH", t)
                                gb.add_relation(oid, tid, "hiring_for",
                                                f"{s.source}:{s.source_id}")

                for person in (s.people or [])[:8]:
                    nm = person.get("name") if isinstance(person, dict) else str(person)
                    pid = gb.add_entity("PERSON", nm or "",
                                        source_doc=f"{s.source}:{s.source_id}")
                    gb.add_relation(wid, pid, "authored_by", f"{s.source}:{s.source_id}")
                    if s.organization:
                        oid = gb.add_entity("ORG", s.organization)
                        gb.add_relation(pid, oid, "affiliated_with",
                                        f"{s.source}:{s.source_id}")

                for t in (s.tags or []):
                    if t and t in _known_tech():
                        tid = gb.add_entity("TECH", t)
                        gb.add_relation(wid, tid, "about", f"{s.source}:{s.source_id}")
        except Exception as exc:
            log.warning("graph: signals layer skipped (%s)", exc)

        # ── Theses ───────────────────────────────────────────────────────────
        try:
            from db.thesis_models import Thesis
            theses = session.query(Thesis).all()
            log.info("graph: %d theses", len(theses))
            for t in theses:
                wid = gb.add_entity(
                    "WORK", f"thesis:{t.source_id}", subtype="thesis",
                    description=(t.title or "")[:900],
                    source_doc=f"thesis:{t.source_id}",
                    meta=(getattr(t, 'url', '') or "")[:300],
                )
                inst = getattr(t, "institution", None)
                if inst:
                    oid = gb.add_entity("ORG", inst, source_doc=f"thesis:{t.source_id}")
                    gb.add_relation(wid, oid, "filed_by", f"thesis:{t.source_id}")
                for a in (getattr(t, "authors", None) or [])[:5]:
                    nm = a.get("name") if isinstance(a, dict) else str(a)
                    pid = gb.add_entity("PERSON", nm or "",
                                        source_doc=f"thesis:{t.source_id}")
                    gb.add_relation(wid, pid, "authored_by", f"thesis:{t.source_id}")
                    if inst:
                        oid = gb.add_entity("ORG", inst)
                        gb.add_relation(pid, oid, "affiliated_with",
                                        f"thesis:{t.source_id}")
                for concept in classify_tech(t.title, getattr(t, "abstract", "")):
                    tid = gb.add_entity("TECH", concept)
                    gb.add_relation(wid, tid, "about", f"thesis:{t.source_id}")
        except Exception as exc:
            log.warning("graph: thesis layer skipped (%s)", exc)


def _known_tech() -> set:
    from neuro_taxonomy import TECH_CONCEPTS
    return set(TECH_CONCEPTS.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Query — the recursive CTE
# ─────────────────────────────────────────────────────────────────────────────

NEIGHBOURHOOD_SQL = """
WITH RECURSIVE walk(id, depth, path) AS (
    SELECT :root, 0, :root
  UNION
    SELECT CASE WHEN r.source_id = w.id THEN r.target_id ELSE r.source_id END,
           w.depth + 1,
           w.path || '>' || CASE WHEN r.source_id = w.id
                                 THEN r.target_id ELSE r.source_id END
    FROM relations r
    JOIN walk w
      ON (r.source_id = w.id OR r.target_id = w.id)
    WHERE w.depth < :max_depth
      AND instr(w.path, CASE WHEN r.source_id = w.id
                             THEN r.target_id ELSE r.source_id END) = 0
)
SELECT e.id, e.name, e.type, e.subtype, MIN(w.depth) AS depth, e.weight
FROM walk w JOIN entities e ON e.id = w.id
GROUP BY e.id
ORDER BY depth, e.weight DESC
LIMIT :limit
"""


def resolve(conn: sqlite3.Connection, term: str) -> tuple[str, str] | None:
    """Find an entity by exact name, alias, then substring."""
    cur = conn.cursor()
    for sql, arg in (
        ("SELECT id,name FROM entities WHERE LOWER(name)=LOWER(?)", term),
        ("SELECT e.id,e.name FROM aliases a JOIN entities e ON e.id=a.entity_id "
         "WHERE LOWER(a.alias)=LOWER(?)", term),
        ("SELECT id,name FROM entities WHERE LOWER(name) LIKE LOWER(?) "
         "ORDER BY weight DESC LIMIT 1", f"%{term}%"),
    ):
        row = cur.execute(sql, (arg,)).fetchone()
        if row:
            return row
    return None


def explain(conn: sqlite3.Connection, term: str, depth: int = 2) -> str:
    hit = resolve(conn, term)
    if not hit:
        return f"No entity matching {term!r}."
    eid, name = hit
    cur = conn.cursor()
    rows = cur.execute(NEIGHBOURHOOD_SQL,
                       {"root": eid, "max_depth": depth, "limit": 60}).fetchall()

    out = [f"{name}  —  {len(rows) - 1} connected entities within {depth} hops", ""]
    direct = cur.execute("""
        SELECT r.predicate, e.name, e.type, e.subtype, r.weight, r.source_doc
        FROM relations r
        JOIN entities e ON e.id = CASE WHEN r.source_id=:id
                                       THEN r.target_id ELSE r.source_id END
        WHERE r.source_id=:id OR r.target_id=:id
        ORDER BY r.weight DESC LIMIT 25
    """, {"id": eid}).fetchall()
    for pred, nm, typ, sub, w, doc in direct:
        label = f"{typ}/{sub}" if sub else typ
        out.append(f"  {pred:<16} {nm[:58]:<60} [{label}]  x{int(w)}  <- {doc}")

    by_type = Counter(r[2] for r in rows)
    out += ["", "  neighbourhood: " + ", ".join(f"{k} {v}" for k, v in by_type.items())]
    return "\n".join(out)


def shortest_path(conn: sqlite3.Connection, a: str, b: str, max_depth: int = 5) -> str:
    ha, hb = resolve(conn, a), resolve(conn, b)
    if not ha or not hb:
        return f"Could not resolve {'both' if not ha and not hb else (a if not ha else b)!r}."
    row = conn.execute("""
        WITH RECURSIVE walk(id, depth, path) AS (
            SELECT :src, 0, :src
          UNION
            SELECT CASE WHEN r.source_id=w.id THEN r.target_id ELSE r.source_id END,
                   w.depth+1,
                   w.path || '>' || CASE WHEN r.source_id=w.id
                                         THEN r.target_id ELSE r.source_id END
            FROM relations r JOIN walk w
              ON (r.source_id=w.id OR r.target_id=w.id)
            WHERE w.depth < :d
              AND instr(w.path, CASE WHEN r.source_id=w.id
                                     THEN r.target_id ELSE r.source_id END)=0
        )
        SELECT path, depth FROM walk WHERE id=:tgt ORDER BY depth LIMIT 1
    """, {"src": ha[0], "tgt": hb[0], "d": max_depth}).fetchone()

    if not row:
        return f"No path from {ha[1]} to {hb[1]} within {max_depth} hops."
    ids = row[0].split(">")
    names = []
    for i in ids:
        r = conn.execute("SELECT name,type FROM entities WHERE id=?", (i,)).fetchone()
        names.append(f"{r[0]} [{r[1]}]" if r else i[:8])
    return f"{ha[1]} -> {hb[1]}  ({row[1]} hops)\n\n  " + "\n    -> ".join(names)


# ─────────────────────────────────────────────────────────────────────────────
# Demo corpus — lets the graph be built and rendered with no DB at all
# ─────────────────────────────────────────────────────────────────────────────

def build_demo(gb: GraphBuilder) -> None:
    """
    Small synthetic corpus that exercises every node type, every predicate and
    the cross-layer chain. Also the offline fallback if the DB is unreachable.
    """
    P = [
        ("US11938324B2", "Adaptive deep brain stimulation using local field potentials",
         ["Medtronic, Inc."], ["Smith, John A", "Patel, Riya"],
         ["Deep Brain Stimulation", "Closed-Loop Neuromodulation"]),
        ("US11890463B2", "Closed-loop neurostimulation with biomarker detection",
         ["MEDTRONIC INC"], ["Patel, Riya"],
         ["Closed-Loop Neuromodulation"]),
        ("US11724110B2", "Implantable pulse generator with directional leads",
         ["Boston Scientific Neuromodulation Corporation"], ["Nguyen, Trang"],
         ["Deep Brain Stimulation"]),
        ("US11642527B2", "Spinal cord stimulation waveform optimisation",
         ["BOSTON SCIENTIFIC CORP"], ["Nguyen, Trang", "Alvarez, Diego"],
         ["Spinal Cord Stimulation"]),
        ("US11801386B2", "Flexible electrode thread array for cortical recording",
         ["Neuralink Corp."], ["Chen, Wei"],
         ["Brain-Computer Interface", "Neural Recording Hardware"]),
        ("US11957910B2", "Endovascular neural interface delivered via stentrode",
         ["Synchron, Inc."], ["O'Brien, Katherine"],
         ["Brain-Computer Interface"]),
        ("US11685822B2", "High-density microelectrode array for speech decoding",
         ["Paradromics Inc"], ["Chen, Wei", "Okafor, Ada"],
         ["Brain-Computer Interface", "Neural Recording Hardware"]),
    ]
    for pid, title, orgs, invs, techs in P:
        w = gb.add_entity("WORK", pid, subtype="patent", description=title,
                          source_doc=f"patent:epo:{pid}", meta=title)
        for o in orgs:
            oid = gb.add_entity("ORG", o, source_doc=f"patent:{pid}")
            gb.add_relation(w, oid, "filed_by", f"patent:{pid}")
        for i in invs:
            iid = gb.add_entity("PERSON", i, source_doc=f"patent:{pid}")
            gb.add_relation(w, iid, "authored_by", f"patent:{pid}")
            for o in orgs[:1]:
                gb.add_relation(iid, gb.add_entity("ORG", o),
                                "affiliated_with", f"patent:{pid}")
        for t in techs:
            gb.add_relation(w, gb.add_entity("TECH", t), "about", f"patent:{pid}")

    TH = [
        ("thesis:2023:0021", "Decoding attempted speech from intracortical arrays",
         "Stanford University", ["Chen, Wei"], ["Brain-Computer Interface"]),
        ("thesis:2022:0104", "Biomarker-driven control policies for adaptive DBS",
         "University of California", ["Patel, Riya"],
         ["Deep Brain Stimulation", "Closed-Loop Neuromodulation"]),
    ]
    for tid, title, inst, authors, techs in TH:
        w = gb.add_entity("WORK", tid, subtype="thesis", description=title,
                          source_doc=tid, meta=title)
        oid = gb.add_entity("ORG", inst, source_doc=tid)
        gb.add_relation(w, oid, "filed_by", tid)
        for a in authors:
            pid_ = gb.add_entity("PERSON", a, source_doc=tid)
            gb.add_relation(w, pid_, "authored_by", tid)
            gb.add_relation(pid_, oid, "affiliated_with", tid)
        for t in techs:
            gb.add_relation(w, gb.add_entity("TECH", t), "about", tid)

    G = [
        ("nih:5R01NS128340", "Closed-loop DBS for treatment-resistant depression",
         "University of California", 2_450_000, ["Closed-Loop Neuromodulation"]),
        ("nih:5U01NS123456", "Speech neuroprosthesis pivotal feasibility",
         "Stanford University", 3_100_000, ["Brain-Computer Interface"]),
    ]
    for gid, title, org, amt, techs in G:
        w = gb.add_entity("WORK", gid, subtype="grant", description=title,
                          source_doc=gid, meta=f"${amt:,}")
        gb.add_relation(w, gb.add_entity("ORG", org, source_doc=gid),
                        "funded_by", gid)
        for t in techs:
            gb.add_relation(w, gb.add_entity("TECH", t), "about", gid)

    TR = [("NCT06120491", "Early feasibility study of an endovascular BCI",
           "Synchron, Inc.", ["Brain-Computer Interface"]),
          ("NCT05938413", "Adaptive DBS versus continuous DBS in Parkinson disease",
           "Medtronic, Inc.", ["Deep Brain Stimulation", "Closed-Loop Neuromodulation"])]
    for nct, title, sponsor, techs in TR:
        w = gb.add_entity("WORK", nct, subtype="trial", description=title,
                          source_doc=nct, meta=title)
        gb.add_relation(w, gb.add_entity("ORG", sponsor, source_doc=nct),
                        "sponsored_by", nct)
        for t in techs:
            gb.add_relation(w, gb.add_entity("TECH", t), "about", nct)

    FDA = [("K243117", "Ceribell Seizure Detection Software", "Ceribell, Inc.",
            ["EEG & Seizure"]),
           ("P960009", "Vercise Deep Brain Stimulation System",
            "Boston Scientific Corp", ["Deep Brain Stimulation"])]
    for k, title, org, techs in FDA:
        w = gb.add_entity("WORK", k, subtype="clearance", description=title,
                          source_doc=f"fda:{k}", meta=title)
        gb.add_relation(w, gb.add_entity("ORG", org, source_doc=f"fda:{k}"),
                        "filed_by", f"fda:{k}")
        for t in techs:
            gb.add_relation(w, gb.add_entity("TECH", t), "about", f"fda:{k}")

    JOBS = [("jobs:synchron:4411", "Director, Regulatory Affairs", "Synchron, Inc.",
             ["Brain-Computer Interface"]),
            ("jobs:paradromics:2210", "Senior Reimbursement Strategy Lead",
             "Paradromics Inc", ["Brain-Computer Interface"]),
            ("jobs:neuralink:7781", "Clinical Trial Manager", "Neuralink Corp.",
             ["Brain-Computer Interface"])]
    for jid, title, org, techs in JOBS:
        w = gb.add_entity("WORK", jid, subtype="posting", description=title,
                          source_doc=jid, meta=title)
        oid = gb.add_entity("ORG", org, source_doc=jid)
        gb.add_relation(w, oid, "filed_by", jid)
        for t in techs:
            gb.add_relation(oid, gb.add_entity("TECH", t), "hiring_for", jid)

    ART = [("rss:nt_notables:60", "Notables #60: BCI funding accelerates",
            "Neurotech Notables (Naveen Rao)",
            ["Brain-Computer Interface", "Deep Brain Stimulation"])]
    for aid, title, pub, techs in ART:
        w = gb.add_entity("WORK", aid, subtype="article", description=title,
                          source_doc=aid, meta=title)
        gb.add_relation(w, gb.add_entity("ORG", pub, source_doc=aid),
                        "filed_by", aid)
        for t in techs:
            gb.add_relation(w, gb.add_entity("TECH", t), "about", aid)

    # thesis -> patent maturity chain
    gb.add_relation(gb.add_entity("WORK", "thesis:2023:0021", subtype="thesis"),
                    gb.add_entity("WORK", "US11685822B2", subtype="patent"),
                    "advances_to", "analysis:author-continuity")


# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Build the NIA knowledge graph")
    ap.add_argument("--out", default="nia_graph.sqlite")
    ap.add_argument("--demo", action="store_true",
                    help="build from a synthetic corpus (no DB required)")
    ap.add_argument("--explain", metavar="TERM")
    ap.add_argument("--path", nargs=2, metavar=("A", "B"))
    ap.add_argument("--depth", type=int, default=2)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(name)s  %(message)s")

    # Query modes read an existing graph.
    if args.explain or args.path:
        conn = sqlite3.connect(args.out)
        print(explain(conn, args.explain, args.depth) if args.explain
              else shortest_path(conn, *args.path))
        conn.close()
        return 0

    import os
    if os.path.exists(args.out):
        os.remove(args.out)          # full rebuild — IDs are deterministic

    gb = GraphBuilder(args.out)
    if args.demo:
        build_demo(gb)
    else:
        try:
            build_from_db(gb)
        except Exception as exc:
            log.error("graph: DB build failed (%s)", exc)
            log.error("graph: run with --demo to build from the synthetic corpus")
            gb.close()
            return 1

    st = gb.finalise()
    gb.close()

    print(f"\n  graph written -> {args.out}")
    print(f"  entities   {st.entities:>6}   {st.by_type}")
    print(f"  relations  {st.relations:>6}   {st.by_predicate}")
    print(f"  aliases    {st.aliases:>6}   ({st.merged_orgs} entities had >1 spelling merged)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
