"""
Re-score the stored corpus against the CURRENT taxonomy.

The relevance gate runs at ingest time, which means every taxonomy change
applies only to records arriving afterwards. Everything admitted under the old
rules stays admitted forever. That is not a subtle problem: `A61B5/1455`
(pulse oximetry — present in every smartwatch) was in the adjacent-CPC list for
part of one day, and the consumer-electronics patents it let in are still the
top-ranked "neurotech" assignees on the live site, long after the code that
admitted them was removed.

So a gate change is only half a fix. This is the other half: re-run the current
rules over what is already stored, and report — or remove — whatever no longer
qualifies.

DRY RUN BY DEFAULT. It shows exactly what would go, grouped by reason and with
worked examples, and changes nothing until `--apply`. Deletion is safe here in
the sense that matters: every record is re-ingestible from its source API, so
the cost of being wrong is one pipeline run, not lost data.

Usage:
    python main.py reindex              # report only
    python main.py reindex --apply      # actually remove
    python main.py reindex --limit 500  # sample, for a quick look
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter

from neuro_taxonomy import score_record

ACCEPT = ("core", "adjacent")


def _examples(rows, n=4):
    out = []
    for title, reason in rows[:n]:
        t = (title or "(untitled)")[:66]
        out.append(f"      - {t}\n          why dropped: {reason}")
    return "\n".join(out)


def run(apply: bool = False, limit: int | None = None) -> int:
    from db import get_session
    from db.models import RawPatent
    from db.signal_models import Signal

    print("\n  Re-scoring the stored corpus against the current taxonomy.")
    print("  " + ("APPLYING CHANGES" if apply else "DRY RUN — nothing will be modified"))

    drop_patents, drop_signals = [], []
    reasons = Counter()
    tier_before_after = Counter()
    kept = Counter()

    with get_session() as s:
        q = s.query(RawPatent)
        if limit:
            q = q.limit(limit)
        pats = q.all()
        print(f"\n  patents in corpus : {len(pats):,}")
        for p in pats:
            r = score_record(p.title, p.abstract,
                             (p.cpc_codes or []) + (p.ipc_codes or []))
            was = ((p.raw_payload or {}).get("relevance_tier") or "?")
            if r.tier in ACCEPT:
                kept[r.tier] += 1
                if was != r.tier:
                    tier_before_after[f"{was} -> {r.tier}"] += 1
            else:
                why = r.reasons[0] if r.reasons else "no neurotech signal"
                reasons[why] += 1
                drop_patents.append((p.id, p.title, why))

        q = s.query(Signal)
        if limit:
            q = q.limit(limit)
        sigs = q.all()
        print(f"  signals in corpus : {len(sigs):,}")
        PRE_FILTERED = ("rss:", "jobs:", "arxiv", "biorxiv", "medrxiv", "sec_edgar")
        for sig in sigs:
            # Sources that gate themselves inside their own ingestor are not
            # re-judged here, exactly as in the live pipeline.
            if any((sig.source or "").startswith(p) for p in PRE_FILTERED):
                kept["pre-filtered source"] += 1
                continue
            r = score_record(sig.title, sig.summary, list(sig.tags or []))
            if r.tier in ACCEPT:
                kept[r.tier] += 1
            else:
                why = r.reasons[0] if r.reasons else "no neurotech signal"
                reasons[why] += 1
                drop_signals.append((sig.id, sig.title, why))

    total = len(pats) + len(sigs)
    n_drop = len(drop_patents) + len(drop_signals)

    print(f"\n  {'':<44}{'COUNT':>8}")
    print("  " + "-" * 54)
    for k, v in kept.most_common():
        print(f"  keep · {k:<38}{v:>8,}")
    print(f"  {'DROP — no longer passes the gate':<44}{n_drop:>8,}")
    print("  " + "-" * 54)
    print(f"  {'total':<44}{total:>8,}   ({100*n_drop/max(total,1):.1f}% would be removed)")

    if tier_before_after:
        print("\n  tier changes among records that stay:")
        for k, v in tier_before_after.most_common(6):
            print(f"    {k:<28} {v:>6,}")

    if reasons:
        print("\n  why records are being dropped:")
        for why, n in reasons.most_common(8):
            print(f"    {n:>6,}  {why}")

    if drop_patents:
        print(f"\n  example patents to be removed ({len(drop_patents):,} total):")
        print(_examples([(t, w) for _i, t, w in drop_patents]))
    if drop_signals:
        print(f"\n  example signals to be removed ({len(drop_signals):,} total):")
        print(_examples([(t, w) for _i, t, w in drop_signals]))

    if not apply:
        print("\n  Nothing changed. Re-run with --apply to remove these.")
        print("  Everything removable is re-ingestible from source, so the cost")
        print("  of getting this wrong is one pipeline run.")
        return 0

    if not n_drop:
        print("\n  Nothing to remove — the corpus already matches the taxonomy.")
        return 0

    with get_session() as s:
        for pid, _t, _w in drop_patents:
            obj = s.get(RawPatent, pid)
            if obj is not None:
                s.delete(obj)
        for sid, _t, _w in drop_signals:
            obj = s.get(Signal, sid)
            if obj is not None:
                s.delete(obj)
    print(f"\n  Removed {n_drop:,} records.")
    print("  Rebuild the site so the change is visible:  python build_site.py")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="actually remove (default is a dry run)")
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    try:
        return run(apply=a.apply, limit=a.limit)
    except Exception as exc:
        print(f"  database unavailable: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
