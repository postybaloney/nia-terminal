<!--
═══════════════════════════════════════════════════════════════════════════════
DRAFT — NOT YET PUBLISHABLE. DELETE THIS BLOCK BEFORE PUBLISHING.

Working draft prepared from research. Not legal advice. Not lawyer-reviewed.

The load-bearing part of this document is section 3. Its job is to establish
that the sentiment scores measure THE TONE OF CITED COVERAGE, not the quality,
finances, or prospects of any company. That framing is what keeps the scores on
the protected-opinion side of Milkovich v. Lorain Journal, and it is why the
wording throughout is "coverage was negative" rather than anything that reads
as a rating OF the company. Do not let that drift in future edits — a score
presented as measuring the company itself starts to look like a verifiable
factual claim about creditworthiness, which is defamation per se territory.

Set the four LEGAL_* environment variables; build_legal.py refuses blanks.

RESOLVED 11 Sep 2026 — the ⚠ OPEN item that used to sit in section 3 is closed.
verify_grounding() in affect.py now requires an exact substring match against
the normalised source and drops anything else, so section 3's claim that the
displayed passage is a real quote is true. It was NOT true when this was
drafted, and it is the one sentence here that must be re-checked if that
function is ever loosened.
═══════════════════════════════════════════════════════════════════════════════
-->

# Terms of Use — NIA Terminal

**Last updated: ‹DATE›**

The NIA Terminal is a personal, non-commercial research project. It is
published for free, carries no advertising, sells nothing, and is not a product
or a service. By using it you accept the terms below.

---

## 1. What this is

An automated index of public records about neurotechnology — patents, doctoral
theses, preprints, research grants, clinical trials, regulatory clearances,
public company filings, industry press, and job postings — assembled nightly
and presented with derived analysis on top.

**Every record links back to its source.** The sources are authoritative; this
site is not. Where they disagree, the source is right.

## 2. No warranty, and no reliance

The Terminal is provided **as is**, with no warranties of any kind, express or
implied, including as to accuracy, completeness, currency, merchantability, or
fitness for any purpose.

Records are ingested and processed **automatically, without human review**.
Automated systems misclassify, mismatch, and misattribute. Assume this site
contains errors, because it does.

**Do not rely on anything here for any decision that matters.** In particular,
nothing on this site is:

- investment advice, or any basis for a decision to buy or sell a security
- medical advice, or a basis for any clinical or treatment decision
- legal advice
- a due-diligence record, a credit assessment, or an evaluation of any
  organisation's financial condition, solvency, or prospects
- an assessment of any individual's competence, character, or professional
  standing

## 3. How the analysis layers work, and what they mean

Three derived layers appear alongside the raw records. Because they are the
part of the site most easily misread, here is precisely what each one claims.

### Coverage sentiment

The site publishes a sentiment label and a numeric valence for organisations
named in news articles and job postings.

> **This measures the tone of the cited coverage. It does not measure the
> organisation.**

A negative valence means the article we quote is written in negative terms. It
is **not** a statement that the organisation performed badly, that its products
are unsafe, that it is in financial difficulty, or that it did anything wrong.
An organisation may be covered negatively while doing well, and covered
positively while doing badly.

These values are produced by a large language model reading the cited text. The
model applies its own weighting; two systems reading the same article would
reasonably produce different numbers, and no number here is a measurement of an
objective quantity. **They are automated interpretations, not findings of
fact.**

Every sentiment value is displayed with the quoted passage it was derived from
and a link to the source. **Read the passage and the source rather than the
number.** Where the passage and the number seem to disagree, the passage is the
evidence and the number is our system's fallible reading of it.

Every published passage is a **literal substring of the source document**,
checked automatically before it is shown. Where a passage cannot be quoted
exactly, the entity is dropped rather than paraphrased — so a passage shown in
quotation marks on this site is what the source actually said, and nothing
appears here that could not be verified against the original.

### Establishment and Frontier scores

Two percentile scores for organisations and technologies, reflecting how much
independent corroboration exists for something and how structurally unusual its
connections are.

These are **subjective composites**. They weight several inputs against each
other according to choices we made, and different reasonable choices produce
different rankings. They rank entities *relative to each other within our
corpus* — which is partial, biased toward what our sources happen to cover, and
not a census of the field. A low score means our corpus holds little evidence,
which is a statement about our corpus, not about the entity.

### The knowledge graph

Connections are inferred automatically from co-occurrence in records. **An edge
means two things appeared together in a document we indexed.** It does not
imply a business relationship, endorsement, partnership, or any other
connection in the world.

## 4. Corrections

If anything here is wrong, tell us and it will be fixed or removed.

**‹CORRECTIONS@YOURDOMAIN›**

Corrections concerning a named individual, or a factual claim about a named
organisation, are treated as urgent and actioned within one business day. You
do not need to explain why you are asking. See the
[Privacy Notice](‹LINK›) for removal of personal data.

## 5. Sources, and the absence of endorsement

Data is reproduced from public sources under their respective terms; see
[Sources & Attribution](‹LINK›) for the full list and required credits.

**No source endorses this site.** No government agency, patent office, journal,
preprint server, publisher, or company named on this site has reviewed,
approved, sponsored, or is affiliated with it. Where a source requires specific
disclaimer language, that language appears on the Sources page and governs.

Records reproduced from U.S. Government sources are used on the basis that they
are public-domain works; that use does not imply endorsement by any agency.

## 6. Intellectual property

Titles, abstracts, and quoted passages remain the property of their respective
rights holders and are reproduced here for the purpose of analysis and
commentary, with attribution and a link to the original in every case.

**If you hold rights in material reproduced here and want it removed, email
‹CORRECTIONS@YOURDOMAIN› and it will be removed.** You do not need to send a
formal notice, and no counter-argument will be made. The fastest route to
removal is the direct one.

The analysis, scoring methodology, code, and presentation are the operator's
own work.

## 7. Acceptable use

You may read this site, link to it, and cite it. You may not use it to harass,
profile, or make decisions about any individual named on it, and you may not
represent its output as fact, as verified, or as endorsed by any source.

## 8. Liability

To the fullest extent permitted by law, the operator is not liable for any loss
or damage arising from use of, or reliance on, this site.

## 9. Governing law

These terms are governed by the laws of ‹YOUR STATE›, United States, without
regard to conflict-of-laws principles.

## 10. Changes

These terms may change. The "last updated" date reflects the current version.

---

<!--
REMOVE BEFORE PUBLISHING — implementation notes

  ▸ Section 3 is load-bearing. Its protection comes from the framing, not from
    the disclaimer. Labelling something "automated" confers no legal protection
    by itself; what does the work is (a) framing the score as about COVERAGE
    rather than about the company, (b) displaying the underlying evidence
    alongside every score, and (c) making clear the weighting is subjective.
    All three must remain true on the actual page, not just in this document.

  ▸ Do NOT add a "most negative sentiment" leaderboard or any superlative
    ranking of companies by negativity. A per-article score is defensible
    commentary; a ranking naming the "worst" company is a different and much
    worse posture.

  ▸ Section 3's verbatim claim is only as true as verify_grounding(). If that
    function is ever relaxed to admit paraphrases again, this section becomes
    a false statement in a legal document. affect.py --selftest covers the
    three cases that matter (entity swap, negation flip, word salad); treat a
    failure there as a documentation bug as well as a code one.

  ▸ Section 6's promise of removal on request is deliberate. For a hobby
    project the cost of complying instantly is near zero and the cost of a
    DMCA notice to GitHub — which takes the whole site down without anyone
    assessing the merits — is total. Never contest a takedown request here.
-->
