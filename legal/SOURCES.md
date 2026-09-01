<!--
═══════════════════════════════════════════════════════════════════════════════
DRAFT — DELETE THIS BLOCK BEFORE PUBLISHING.

Unlike the other two documents, most of this one is NOT optional and NOT a
matter of legal judgment. The attribution strings and disclaimers below are
required by the terms you already accepted when you took the data:

  ▸ ClinicalTrials.gov imposes FOUR mandatory conditions — attribute the
    source, keep data current, DISPLAY THE PROCESSING DATE, and describe
    modifications. The site currently satisfies none of them.
  ▸ NLM requires the exact phrase "Courtesy of the U.S. National Library of
    Medicine" and either currency or a conspicuous not-current notice.
  ▸ Google Patents BigQuery is CC BY 4.0 and requires crediting BOTH IFI
    CLAIMS Patent Services AND Google.
  ▸ openFDA requires its medical disclaimers be carried.

Publishing this page is the fix for audit findings 05 and 10.

The ‹PROCESSING DATE› fields must be generated, not hand-written — a stale
hardcoded date is itself a breach of the ClinicalTrials.gov condition. See the
implementation note at the foot of this file.
═══════════════════════════════════════════════════════════════════════════════
-->

# Sources & Attribution — NIA Terminal

**Data last processed: ‹PROCESSING DATE — GENERATED, NOT HAND-WRITTEN›**

Everything on the NIA Terminal comes from the public sources listed below.
Required credits and disclaimers appear with each. Where a source's own terms
require particular wording, that wording is reproduced exactly and governs over
anything else on this site.

**No source listed here endorses, sponsors, reviews, or is affiliated with this
site.**

---

## How the data is modified

Several sources require that modifications be described. Applied to every
source, the pipeline:

1. **Filters** records against a neurotechnology relevance vocabulary; records
   that do not pass are excluded and counted, not silently dropped.
2. **Normalises and merges** organisation and person names across sources, so
   that variant spellings of the same entity resolve to one record. This
   matching is automatic and can be wrong.
3. **Truncates** titles and abstracts for display.
4. **Derives** additional values not present in any source — relevance scores,
   establishment and frontier percentiles, coverage-sentiment values, and
   inferred graph connections. These are **our** output, not the source's, and
   must not be attributed to any source.
5. **Caches** records between runs. Data is refreshed nightly and is therefore
   **not guaranteed to reflect the current state of any source.** Always follow
   the link to the source for the authoritative record.

---

## Patents

### European Patent Office — Open Patent Services

Bibliographic patent data retrieved via the EPO's Open Patent Services API,
used under the OPS Terms and Conditions and Fair Use Charter. The EPO does not
endorse this site and is not responsible for its content or for any processing
applied to the data here.

### Google Patents Public Data

> **"Google Patents Public Data" by IFI CLAIMS Patent Services and Google,
> used under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).**

Modified as described above. This credit is required by the licence and both
parties must be named.

---

## Research funding & clinical trials

### NIH RePORTER

Federal award data from the NIH RePORTER API, a work of the United States
Government. Reference on this site to any product, process, service,
manufacturer, or company does not constitute endorsement or recommendation by
the U.S. Government or the National Institutes of Health.

### ClinicalTrials.gov

Trial registration data from ClinicalTrials.gov.

> Courtesy of the U.S. National Library of Medicine.

**Data processed by ClinicalTrials.gov on ‹PROCESSING DATE›.** Records here are
modified as described above.

**This site does not reflect the most current or most accurate data available
from the National Library of Medicine.** Data is refreshed on a nightly cycle
and may lag the source. For the authoritative and current record, follow the
link on any trial to ClinicalTrials.gov.

Neither the United States Government, the U.S. Department of Health and Human
Services, the National Institutes of Health, the National Library of Medicine,
nor any of their agencies, contractors, subcontractors or employees make any
warranties, express or implied, with respect to data obtained from this
database. The NLM does not endorse this site.

---

## Regulatory

### openFDA

Device clearance, approval, and adverse-event data provided by the U.S. Food
and Drug Administration via openFDA (<https://open.fda.gov>), used under
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).

> **Do not rely on openFDA — or on this site — to make decisions regarding
> medical care. Always speak to your health provider about the risks and
> benefits of FDA-regulated products.**

Content retrieved from openFDA has neither been altered nor verified by the
FDA, and not all data in openFDA has been validated for clinical or production
use. Where adverse-event data appears, note that **there is no certainty that a
reported event was actually caused by the product**; such reports have not been
verified as to any cause-and-effect relationship and cannot be used to estimate
the incidence of any event. The FDA makes no warranty that the data is
error-free.

### U.S. Securities and Exchange Commission — EDGAR

Filing data from SEC EDGAR, a work of the United States Government, free to
access and reuse. The SEC does not endorse this site.

---

## Academic literature

### arXiv

Article metadata from the arXiv API, used under
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) in accordance
with the arXiv API Terms of Use. Only descriptive metadata is reproduced —
titles, authors, identifiers, and dates. **Full texts are not stored or served
here**; every entry links to arXiv for the article itself.

Thank you to arXiv for use of its open access interoperability. arXiv does not
endorse this site.

### Doctoral theses

Metadata from public university and national thesis repositories. Full texts
are not stored or served; entries link to the holding repository. Rights in
each thesis remain with its author and institution.

---

## Industry press and job postings

Article titles, bylines, publication names, and dates are reproduced with a
link to the original in every case, for the purpose of analysis and commentary.
**Full article texts are not reproduced.** Copyright in each article remains
with its author or publisher.

Job postings are indexed by employer, role, and location. **No individual
applicant, recruiter, or hiring manager is named.** Posting text remains the
property of the employer.

Rights holders who want material removed should email
**‹CORRECTIONS@YOURDOMAIN›**. It will be removed without argument and no formal
notice is required.

---

## Corrections

Anything on this page that is wrong — a missing credit, an incorrect
attribution, a disclaimer that has changed — should be reported to
**‹CORRECTIONS@YOURDOMAIN›** and will be corrected.

---

<!--
REMOVE BEFORE PUBLISHING — implementation notes

  ▸ ‹PROCESSING DATE› MUST be generated by build_site.py, not typed. A
    hardcoded date goes stale on the first nightly run and a stale processing
    date is worse than none — it is an affirmatively false statement made to
    satisfy a condition you accepted. build_snapshot.py already computes
    `generated_at`; thread it through to this page.

  ▸ Ideally, stamp a per-source processing date rather than one global one,
    since sources are fetched in sequence and a failed ingestor means that
    source did not refresh at all. IngestRun already records this.

  ▸ The claim "full article texts are not reproduced" is NOT currently true.
    feeds.py stores summary text from RSS, and free-post Substack feeds carry
    the entire article body. Either make the sentence true by trimming what is
    stored, or delete the sentence. Do not publish it as-is. This is audit
    finding 06.

  ▸ The claim "no individual applicant, recruiter, or hiring manager is named"
    IS currently true — jobs.py sets people=[]. Keep it that way.

  ▸ If BigQuery ingestion ever pulls a table other than the publications
    tables, re-check its licence. ChEMBL in the same project is CC BY-SA 3.0,
    which is share-alike and would impose obligations on your own output.

  ▸ Link this page from the footer of all three generated pages, not just from
    one. build_site.py's inject_nav() is the place to do it.
-->
