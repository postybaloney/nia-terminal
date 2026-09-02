"""
build_legal.py — render the privacy notice, terms and sources page.

Three things this does that a plain markdown-to-HTML step would not:

  1. REFUSES TO BUILD with an unfilled placeholder. Every ‹ANGLE BRACKET›
     token must be resolved or the build fails and names the ones that are
     missing. A legal page that ships with "‹YOUR EMAIL›" on it is worse than
     no page: it is a public, dated commitment that visibly cannot be kept.

  2. GENERATES the processing date rather than letting it be typed.
     ClinicalTrials.gov's terms require the date the data was processed to be
     displayed. A hardcoded date is correct for exactly one day and is then an
     affirmatively false statement made in order to satisfy a condition — worse
     than omitting it.

  3. STRIPS the editorial comment blocks. The source markdown carries
     <!-- DRAFT --> notes for the author. In HTML those become invisible
     comments that ship to the reader inside the page source, which is how
     internal notes end up quoted back at people. They are removed explicitly
     and the build asserts that none survived.

The markdown files under legal/ stay the source of truth so the wording can be
reviewed and diffed as prose rather than as HTML.
"""
from __future__ import annotations

import argparse
import html
import os
import re
import sys
from datetime import datetime, timezone

# ── Palette: matches build_snapshot.py so the pages read as one site ─────────
BG = "#050810"
CARD = "#0d1117"
CARD2 = "#131a24"
BORDER = "#1f2937"
TEXT = "#e5e7eb"
DIM = "#8b98a9"
AMBER = "#f5a524"
MONO = "'Courier New', ui-monospace, Menlo, monospace"

PAGES = [
    ("legal/PRIVACY.md", "privacy.html", "Privacy Notice"),
    ("legal/TERMS.md", "terms.html", "Terms of Use"),
    ("legal/SOURCES.md", "sources.html", "Sources & Attribution"),
]

_PLACEHOLDER = re.compile(r"‹([^›]+)›")
_COMMENT = re.compile(r"<!--.*?-->", re.S)


def substitutions(args) -> dict:
    """
    Values that replace the ‹PLACEHOLDER› tokens.

    Kept in one place and passed in from the command line so the same markdown
    can be rendered for review with obvious dummy values, or for publication
    with the real ones, without editing the prose.
    """
    now = datetime.now(timezone.utc)
    return {
        "DATE": now.strftime("%d %B %Y"),
        "PROCESSING DATE — GENERATED, NOT HAND-WRITTEN":
            now.strftime("%d %B %Y, %H:%M UTC"),
        "PROCESSING DATE": now.strftime("%d %B %Y"),
        "YOUR FULL LEGAL NAME": args.controller,
        "CITY, STATE": args.location,
        "YOUR STATE": args.state,
        "PRIVACY@YOURDOMAIN — a real, monitored address": args.contact,
        "PRIVACY@YOURDOMAIN": args.contact,
        "CORRECTIONS@YOURDOMAIN": args.contact,
        "LINK": "#",
    }


def resolve(md: str, subs: dict, source: str) -> str:
    md = _COMMENT.sub("", md)

    def _one(m):
        key = m.group(1)
        # An EMPTY substitution is not a substitution. Without this the
        # command-line defaults of "" resolved every placeholder to nothing and
        # the build cheerfully produced a privacy notice with no controller and
        # no contact address — precisely the document this function exists to
        # prevent. Missing and blank must fail identically.
        if subs.get(key):
            return subs[key]
        return m.group(0)

    md = _PLACEHOLDER.sub(_one, md)

    left = sorted(set(_PLACEHOLDER.findall(md)))
    if left:
        raise SystemExit(
            f"\n  REFUSING TO BUILD {source}\n\n"
            f"  {len(left)} placeholder(s) still unfilled:\n"
            + "".join(f"      ‹{k}›\n" for k in left)
            + "\n  Publishing a legal page with a visible placeholder is worse "
              "than not\n  publishing one. Pass the value on the command line "
              "or edit the markdown.\n"
        )
    assert "<!--" not in md, f"{source}: an editorial comment survived stripping"
    return md


# ── minimal, deliberate markdown subset ─────────────────────────────────────
# Only what these three documents actually use. A general markdown library
# would be another dependency and another attacker-reachable parser for a page
# whose whole point is being conservative.

def md_to_html(md: str) -> str:
    # The page template already renders the title as an <h1>. Keeping the
    # markdown's own leading H1 would print it twice and give the document two
    # top-level headings, which is both ugly and wrong for a screen reader.
    md = re.sub(r"\A\s*#\s+[^\n]*\n", "", md)
    out, in_ul, in_table, in_quote = [], False, False, False

    def close_blocks():
        nonlocal in_ul, in_table, in_quote
        if in_ul:
            out.append("</ul>"); in_ul = False
        if in_table:
            out.append("</tbody></table></div>"); in_table = False
        if in_quote:
            out.append("</blockquote>"); in_quote = False

    def inline(t: str) -> str:
        t = html.escape(t)
        t = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", t)
        t = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"<i>\1</i>", t)
        t = re.sub(r"`([^`]+?)`", r"<code>\1</code>", t)
        # Links: only http(s) and same-page anchors reach an href.
        def _link(m):
            label, href = m.group(1), html.unescape(m.group(2))
            if not re.match(r"^(https?://|#|/)", href):
                return label
            return (f'<a href="{html.escape(href, quote=True)}"'
                    + (' target="_blank" rel="noopener noreferrer"'
                       if href.startswith("http") else "")
                    + f">{label}</a>")
        return re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, t)

    for raw in md.split("\n"):
        ln = raw.rstrip()

        if not ln.strip():
            close_blocks(); continue
        if ln.strip() == "---":
            close_blocks(); out.append("<hr>"); continue

        if ln.startswith("|"):
            cells = [c.strip() for c in ln.strip("|").split("|")]
            if all(re.fullmatch(r":?-{2,}:?", c) for c in cells if c):
                continue                      # separator row
            if not in_table:
                out.append('<div class="tw"><table><tbody>'); in_table = True
            tag = "th" if len(out) and out[-1].endswith("<tbody>") else "td"
            out.append("<tr>" + "".join(
                f"<{tag}>{inline(c)}</{tag}>" for c in cells) + "</tr>")
            continue

        if ln.startswith(">"):
            if not in_quote:
                close_blocks(); out.append("<blockquote>"); in_quote = True
            out.append(f"<p>{inline(ln.lstrip('> ').rstrip())}</p>")
            continue

        m = re.match(r"^(#{1,4})\s+(.*)$", ln)
        if m:
            close_blocks()
            lvl = len(m.group(1))
            text = m.group(2)
            # Anchor ids so in-document links ("skip to section 7") resolve.
            slug = re.sub(r"[^a-z0-9]+", "-",
                          re.sub(r"<[^>]+>", "", text).lower()).strip("-")
            out.append(f'<h{lvl} id="{slug}">{inline(text)}</h{lvl}>')
            continue

        m = re.match(r"^\s*[-*]\s+(.*)$", ln)
        if m:
            if not in_ul:
                close_blocks(); out.append("<ul>"); in_ul = True
            out.append(f"<li>{inline(m.group(1))}</li>")
            continue

        if in_ul or in_table or in_quote:
            close_blocks()
        out.append(f"<p>{inline(ln)}</p>")

    close_blocks()
    return "\n".join(out)


def page(title: str, body: str, built: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow, noarchive, noimageindex">
<title>NIA Terminal &mdash; {html.escape(title)}</title>
<style>
  *{{box-sizing:border-box}}
  body{{margin:0;background:{BG};color:{TEXT};font:15px/1.7 {MONO};padding:24px}}
  .wrap{{max-width:78ch;margin:0 auto}}
  header{{border-bottom:1px solid {BORDER};padding-bottom:14px;margin-bottom:28px}}
  .brand{{font-size:14px;letter-spacing:3px;color:{DIM}}}
  .brand b{{color:{AMBER}}}
  h1{{font-size:24px;letter-spacing:.02em;margin:14px 0 0}}
  h2{{font-size:16px;color:{AMBER};margin:38px 0 10px;padding-bottom:6px;
     border-bottom:1px solid {BORDER};letter-spacing:.04em}}
  h3{{font-size:14px;margin:26px 0 8px;color:{TEXT}}}
  p{{margin:0 0 14px}}
  a{{color:{AMBER};text-decoration:none;border-bottom:1px dotted {BORDER}}}
  a:hover{{border-bottom-color:{AMBER}}}
  a:focus-visible{{outline:2px solid {AMBER};outline-offset:2px}}
  ul{{margin:0 0 14px;padding-left:20px}} li{{margin-bottom:7px}}
  li::marker{{color:{DIM}}}
  code{{background:{CARD2};padding:.1em .35em;border-radius:3px;font-size:.92em}}
  blockquote{{margin:18px 0;padding:14px 18px;background:{CARD};
    border-left:3px solid {AMBER};border-radius:0 6px 6px 0}}
  blockquote p:last-child{{margin-bottom:0}}
  .tw{{overflow-x:auto;margin:0 0 18px;border:1px solid {BORDER};
    border-radius:6px;background:{CARD}}}
  table{{border-collapse:collapse;width:100%;font-size:13px;min-width:420px}}
  th{{text-align:left;color:{DIM};font-weight:400;font-size:11px;
    letter-spacing:.08em;text-transform:uppercase;padding:11px 14px;
    border-bottom:1px solid {BORDER}}}
  td{{padding:11px 14px;border-bottom:1px solid {CARD2};vertical-align:top}}
  tr:last-child td{{border-bottom:none}}
  hr{{border:none;border-top:1px solid {BORDER};margin:32px 0}}
  footer{{color:{DIM};font-size:11.5px;margin-top:44px;
    border-top:1px solid {BORDER};padding-top:16px;line-height:1.8}}
  @media (prefers-reduced-motion:reduce){{*{{transition:none!important}}}}
</style></head><body>
<div class="wrap">
  <header>
    <div class="brand"><b>NIA</b> &middot; NEUROTECH INTELLIGENCE TERMINAL</div>
    <h1>{html.escape(title)}</h1>
  </header>
{body}
  <footer>
    Generated by build_legal.py on {html.escape(built)}. The markdown source
    lives in <code>legal/</code> and is the version of record.<br>
    &copy; {datetime.now(timezone.utc).year} Epsilon Solutions LLC. All rights reserved.
  </footer>
</div>
</body></html>"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--out", default="site")
    ap.add_argument("--controller", default="",
                    help="the legal entity or person named as data controller")
    ap.add_argument("--location", default="")
    ap.add_argument("--state", default="")
    ap.add_argument("--contact", default="")
    a = ap.parse_args()

    built = datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")
    subs = substitutions(a)
    os.makedirs(a.out, exist_ok=True)

    for src, dest, title in PAGES:
        if not os.path.exists(src):
            print(f"  !! missing {src}"); return 1
        with open(src, "r", encoding="utf-8") as f:
            md = f.read()
        body = md_to_html(resolve(md, subs, src))
        out = os.path.join(a.out, dest)
        with open(out, "w", encoding="utf-8") as f:
            f.write(page(title, body, built))
        print(f"  {src:<22} -> {out}")

    print("\n  NOT linked from any page. build_site.py's nav is unchanged, so")
    print("  these are reachable only by direct URL until you link them.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
