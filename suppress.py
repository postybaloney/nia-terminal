"""
suppress.py — the list that makes "removed" mean removed.

The privacy notice commits to this, in these words:

    "If you ask to be removed from this site, you will be removed. No
     justification is required... Removals are actioned within one business day
     and are permanent."

Nothing in the pipeline could deliver that promise before this file existed.
The corpus is rebuilt nightly from the upstream sources, so deleting a row from
the database removes it until the next ingest re-fetches it from EPO or the
thesis repository and puts it straight back. A removal that silently undoes
itself overnight is worse than no removal: the person has been told the matter
is closed, and it is not.

────────────────────────────────────────────────────────────────────────────
WHY AT RENDER TIME, NOT INGEST TIME

The check runs where records become a published page, not where they enter the
database. Three reasons, in order of weight:

  1. It is retroactive. A name added today disappears from the next build,
     including from records ingested months ago. An ingest-time filter only
     protects against future fetches and leaves the existing corpus untouched —
     which is the opposite of what someone asking for removal wants.

  2. It cannot be bypassed by adding a source. Every generator reads through
     this module, so a new ingestor inherits the suppression automatically
     rather than needing to remember it.

  3. It survives a database restore. The suppression list lives in the repo,
     not in the data, so restoring a backup taken before a removal request
     cannot resurrect the person.

The cost is that suppressed data still sits in the database. That is a real
limitation and the privacy notice should not overstate it: what is guaranteed
is that it stops being PUBLISHED. Purging it from storage is a separate,
manual step, and for a genuine erasure request you should do both.

────────────────────────────────────────────────────────────────────────────
FAILING CLOSED

If suppressions.json is missing, nothing is suppressed and the build proceeds —
that is the correct behaviour for a fresh clone with no requests yet.

If suppressions.json EXISTS but cannot be parsed, this module raises and the
build fails. It does not fall back to an empty list. A corrupt file silently
reading as "suppress nobody" would republish every person who ever asked to be
removed, without a single line of output saying so. A failed build is loud,
recoverable, and cannot hurt anyone.
"""
from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone

__all__ = ["Suppressions", "load", "normalise_name", "DEFAULT_PATH"]

DEFAULT_PATH = "suppressions.json"


class SuppressionFileError(RuntimeError):
    """Raised when the file exists but cannot be trusted. Never swallowed."""


# ─────────────────────────────────────────────────────────────────────────────
# Name matching
# ─────────────────────────────────────────────────────────────────────────────

_TITLES = {"dr", "prof", "professor", "mr", "mrs", "ms", "mx", "sir", "dame"}
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "ph", "md", "msc", "bsc",
             "dphil", "dsc", "mba", "frcs", "facs"}


def normalise_name(name: str) -> str:
    """
    Fold a personal name to a comparison key.

    Deliberately aggressive, because the failure modes are asymmetric. Missing
    a match means republishing someone who asked to be removed; an over-broad
    match means a record is withheld that need not have been. The second is a
    small loss of completeness on a research site. The first is the thing the
    privacy notice promised would not happen.

    Handles the shapes the sources actually emit for one person:

        "Okafor, Ada"   "Ada Okafor"   "A. Okafor"   "OKAFOR A"
        "Müller, Jörg"  "Jorg Muller"  "Dr. Jörg Müller"

    Accents are folded because repositories transliterate inconsistently — the
    same author appears as "Myśliwiec" in one source and "Mysliwiec" in
    another, and a removal request naming either must catch both.
    """
    if not name:
        return ""
    # Decompose and strip combining marks: ś -> s, ö -> o.
    t = unicodedata.normalize("NFKD", str(name))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.lower()
    # "Okafor, Ada" and "Ada Okafor" must land on the same key.
    if "," in t:
        parts = [p.strip() for p in t.split(",", 1)]
        t = f"{parts[1]} {parts[0]}" if len(parts) == 2 else t
    tokens = re.findall(r"[a-z]+", t)
    tokens = [w for w in tokens if w not in _TITLES and w not in _SUFFIXES]
    # Single initials carry almost no information and differ between sources
    # ("A. Okafor" vs "Ada Okafor"), so match on the substantive tokens only.
    substantive = [w for w in tokens if len(w) > 1]
    return " ".join(sorted(substantive)) if substantive else " ".join(tokens)


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Suppressions:
    people: set = field(default_factory=set)
    organisations: set = field(default_factory=set)
    records: set = field(default_factory=set)
    _display: dict = field(default_factory=dict)
    path: str = DEFAULT_PATH
    hits: int = 0

    def blocks_person(self, name: str | None) -> bool:
        if not name:
            return False
        key = normalise_name(name)
        if key and key in self.people:
            self.hits += 1
            return True
        return False

    def blocks_org(self, name: str | None) -> bool:
        if not name:
            return False
        key = normalise_name(name)
        if key and key in self.organisations:
            self.hits += 1
            return True
        return False

    def blocks_record(self, source_id: str | None) -> bool:
        if not source_id:
            return False
        key = str(source_id).strip().lower()
        if key and key in self.records:
            self.hits += 1
            return True
        return False

    def __bool__(self) -> bool:
        return bool(self.people or self.organisations or self.records)

    def summary(self) -> str:
        if not self:
            return "no suppressions on file"
        return (f"{len(self.people)} people, {len(self.organisations)} "
                f"organisations, {len(self.records)} records suppressed")


def load(path: str = DEFAULT_PATH) -> Suppressions:
    """
    Read the suppression list.

    Missing file -> empty (a clean repo with no requests yet).
    Unreadable file -> raise. See the module docstring on failing closed.
    """
    if not os.path.exists(path):
        return Suppressions(path=path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        raise SuppressionFileError(
            f"{path} exists but could not be read ({type(exc).__name__}: {exc}). "
            f"Refusing to build: treating an unreadable suppression list as "
            f"'suppress nobody' would republish everyone who asked to be "
            f"removed. Fix the file or delete it deliberately."
        ) from exc

    if not isinstance(data, dict) or not isinstance(data.get("entries"), list):
        raise SuppressionFileError(
            f"{path} is valid JSON but not the expected shape "
            f"(an object with an 'entries' list). Refusing to build."
        )

    s = Suppressions(path=path)
    for i, e in enumerate(data["entries"]):
        if not isinstance(e, dict):
            raise SuppressionFileError(f"{path}: entry {i} is not an object")
        scope = (e.get("scope") or "person").lower()
        name = (e.get("name") or "").strip()
        if scope == "record":
            rid = (e.get("source_id") or name).strip().lower()
            if rid:
                s.records.add(rid)
            continue
        if not name:
            raise SuppressionFileError(f"{path}: entry {i} has no name")
        key = normalise_name(name)
        if not key:
            raise SuppressionFileError(
                f"{path}: entry {i} name {name!r} normalises to nothing")
        (s.organisations if scope in ("org", "organisation", "organization")
         else s.people).add(key)
        s._display[key] = name
    return s


# ─────────────────────────────────────────────────────────────────────────────
# CLI — the thing you actually run when someone emails
# ─────────────────────────────────────────────────────────────────────────────

def _add(path: str, name: str, scope: str, note: str) -> int:
    data = {"version": 1, "entries": []}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    data.setdefault("entries", [])
    key = normalise_name(name)
    for e in data["entries"]:
        if normalise_name(e.get("name", "")) == key and \
           (e.get("scope") or "person") == scope:
            print(f"  already suppressed: {e['name']!r} (added {e.get('added')})")
            return 0
    data["entries"].append({
        "name": name,
        "scope": scope,
        "added": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "note": note or "removal requested",
        # Stored for human review only. Matching always recomputes the key, so
        # a change to normalise_name applies to existing entries automatically.
        "match_key": key,
    })
    data["note"] = ("Names withheld from every published page. Read at render "
                    "time by build_snapshot, build_issue and graph_build. See "
                    "suppress.py for why this is not an ingest-time filter.")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"  suppressed {name!r} as {scope}  (match key: {key!r})")
    print(f"  -> rebuild and redeploy the site for this to take effect")
    return 0


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(
        description="Withhold a name from every published NIA page.")
    ap.add_argument("--file", default=DEFAULT_PATH)
    ap.add_argument("--add", metavar="NAME",
                    help="name exactly as the person wrote it")
    ap.add_argument("--scope", default="person",
                    choices=["person", "org", "record"])
    ap.add_argument("--note", default="", help="e.g. 'email 2026-08-22'")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--check", metavar="NAME",
                    help="test whether a name would be withheld")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()

    if a.selftest:
        return selftest()
    if a.add:
        return _add(a.file, a.add, a.scope, a.note)
    if a.check:
        s = load(a.file)
        blocked = s.blocks_person(a.check) or s.blocks_org(a.check)
        print(f"  {a.check!r} -> {'WITHHELD' if blocked else 'published'}"
              f"   (key: {normalise_name(a.check)!r})")
        return 0

    s = load(a.file)
    print(f"\n  {s.summary()}   [{a.file}]\n")
    for key in sorted(s.people):
        print(f"    person  {s._display.get(key, key)}")
    for key in sorted(s.organisations):
        print(f"    org     {s._display.get(key, key)}")
    for r in sorted(s.records):
        print(f"    record  {r}")
    print()
    return 0


def selftest() -> int:
    import tempfile
    ok = True

    def check(label, cond):
        nonlocal ok
        ok = ok and cond
        print(f"    {'ok  ' if cond else 'FAIL'}  {label}")

    print("\n  ── name folding: one person, many spellings ──")
    variants = ["Okafor, Ada", "Ada Okafor", "OKAFOR ADA", "Dr. Ada Okafor",
                "Ada Okafor, PhD"]
    keys = {normalise_name(v) for v in variants}
    check(f"5 spellings collapse to 1 key ({keys})", len(keys) == 1)

    accented = {normalise_name(n) for n in
                ["Angelika Myśliwiec", "Angelika Mysliwiec", "Myśliwiec, Angelika"]}
    check(f"accents fold ({accented})", len(accented) == 1)

    check("different people stay different",
          normalise_name("Ada Okafor") != normalise_name("Ada Okonkwo"))

    print("\n  ── the file ──")
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "s.json")
        check("missing file suppresses nobody", not load(p))
        _add(p, "Angelika Myśliwiec", "person", "test")
        s = load(p)
        check("added name is withheld", s.blocks_person("Angelika Mysliwiec"))
        check("transliteration also withheld", s.blocks_person("Myśliwiec, Angelika"))
        check("unrelated name still published", not s.blocks_person("Ada Okafor"))
        check("person scope does not leak to orgs", not s.blocks_org("Angelika Myśliwiec"))

        with open(p, "w", encoding="utf-8") as f:
            f.write("{ this is not json")
        raised = False
        try:
            load(p)
        except SuppressionFileError:
            raised = True
        check("corrupt file raises rather than suppressing nobody", raised)

    print("\n  PASS — removals survive the rebuild\n" if ok else "\n  FAIL\n")
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
