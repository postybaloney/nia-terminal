<!--
═══════════════════════════════════════════════════════════════════════════════
DRAFT — NOT YET PUBLISHABLE. DELETE THIS BLOCK BEFORE PUBLISHING.

This is a working draft prepared from research, not legal advice, and it has
not been reviewed by a lawyer. Before publishing you must:

  1. Fill every ‹ANGLE-BRACKET PLACEHOLDER› below. There are five.
  2. Decide the two open questions marked ⚠ OPEN in the text.
  3. Have a lawyer read it, ideally in the same session as the EPO terms
     question and the media-liability-insurance conversation.

The single most important thing in this document is not the legal recitation —
it is the removal commitment in section 7. That is what converts almost every
realistic complaint into a five-minute email exchange. Do not weaken it, and do
not publish this document without a monitored address attached to it.
═══════════════════════════════════════════════════════════════════════════════
-->

# Privacy Notice — NIA Terminal

**Last updated: ‹DATE›**

This notice explains what personal data appears on the NIA Terminal, where it
came from, why it is here, and how to have it removed.

If you are here because you found your own name on this site and want it taken
down, skip to [section 7](#7-your-rights). It takes one email and no
explanation is required.

---

## 1. Who is responsible

The NIA Terminal is a non-commercial research project. It sells nothing,
carries no advertising, and is not offered as a product or service.

> ⚠ **OPEN — decide before publishing.** Name the controller correctly. If the
> site is operated by your LLC, the LLC is the controller and its registered
> name belongs in the table below. If the LLC exists for other work and this
> project is personal, you are the controller in your own name. Do not put the
> LLC here for the appearance of separation: it makes the activity look
> commercial, which weakens the non-commercial carve-outs this project
> currently relies on, and it does not shield you from a claim arising from
> something you personally wrote and published.

| | |
|---|---|
| **Controller** | ‹YOUR FULL LEGAL NAME› |
| **Location** | ‹CITY, STATE›, United States |
| **Contact** | ‹PRIVACY@YOURDOMAIN — a real, monitored address› |

There is no data protection officer and none is required at this scale.

> ⚠ **OPEN — for your lawyer.** If GDPR Article 3(2) applies to this site, you
> may owe a written EU representative under Article 27. The "occasional
> processing" derogation probably does not cover a continuously-running nightly
> pipeline. This is the exact and only ground on which LocateFamily.com was
> fined €525,000. Either appoint one, or get a documented view that Article 3(2)
> does not apply. Do not leave it undecided.

## 2. Visitors to this site

**No personal data is collected from you by visiting.** The Terminal is a set
of static HTML files. There are no cookies, no analytics, no tracking pixels,
no fonts or scripts loaded from third parties, no forms, no accounts, and no
logging under our control.

The site is served through GitHub Pages and Cloudflare, both of which process
connection data such as IP addresses for delivery and security purposes under
their own privacy policies. We do not receive, request, or have access to that
data.

## 3. Personal data about people named on this site

This is the part that matters, and it concerns people who never visited.

The Terminal indexes public records about neurotechnology research and
development. Some of those records name individuals. Where they do, the site
may show:

- **Name**, as it appears in the source record
- **Institutional affiliation** — a university, hospital, or company
- **The work itself** — the title of a patent, thesis, preprint, or article,
  its identifier, and its date
- **Derived connections** — links between a person, their institution, and
  technology areas, generated automatically from the records above

**We do not collect or publish**: contact details, email addresses, postal
addresses, phone numbers, photographs, dates of birth, employment history
beyond what a cited record states, or any special-category data (health,
biometrics, ethnicity, political or religious views, sexual orientation, trade
union membership).

**We do not attempt to identify anyone who has published anonymously or under a
pseudonym**, and we do not resolve pseudonyms to real identities.

## 4. Where it came from

GDPR Article 14(2)(f) requires that we name the source. In every case the data
originates from one of the following public sources:

| Source | What we take |
|---|---|
| European Patent Office — Open Patent Services | Inventor and applicant names on published patent applications |
| Google Patents Public Data (BigQuery) | Inventor and assignee names on published patents |
| NIH RePORTER | Named investigators on awarded federal research grants |
| ClinicalTrials.gov | Sponsor and responsible-party names on registered trials |
| openFDA | Applicant names on device clearances and approvals |
| SEC EDGAR | Filer names on public company filings |
| arXiv | Author names and metadata on preprints |
| University and national thesis repositories | Author names on published doctoral theses |
| Public RSS feeds and industry newsletters | Author bylines on published articles |
| Public company job boards | Employer and role only — **never** the name of an individual applicant or recruiter |

Every record on the site links back to the source it came from, so any entry
can be checked against the original.

## 5. Why we publish it, and on what legal basis

**Purpose.** To make the structure of the neurotechnology field legible — which
organisations and technologies are connected, where work is concentrating, and
how research moves from academic publication into patents, trials, and
regulatory clearance. That analysis is only possible if the records retain the
attributions the sources themselves publish.

**Legal basis.** Legitimate interests, GDPR Article 6(1)(f). Our assessment of
the balancing test:

- *The interest*: research into, and public understanding of, a field with
  significant public-health implications. This is a recognised legitimate
  interest.
- *Necessity*: the connections the site exists to show cannot be drawn without
  the attributions that link a work to an institution.
- *The balance*: the data is limited to professional and scholarly activity that
  the individuals concerned published under their own names, in a professional
  capacity, in venues designed to be cited. No contact details, no private-life
  information, no special-category data. Nothing is inferred about any
  individual's competence, character, or conduct.

**We recognise this is not the end of the analysis.** Aggregating public records
into a single profile is a distinct act from the original publication, and the
fact that each individual record was already public does not by itself make the
compilation lawful. That is precisely why section 7 exists and why we act on
removal requests without argument.

**No profiling of individuals for decisions.** The site produces no scores,
rankings, ratings, or assessments about individual people. The scoring layers
described in the [Terms of Use](‹LINK›) apply only to organisations and
technologies, never to a named person, and no automated decision with legal or
similarly significant effect is made about anyone.

## 6. How long, and who else sees it

**Retention.** Records are retained while they remain relevant to the analysis.
There is no fixed period.

**A removal is permanent and survives the rebuild.** The corpus is reassembled
nightly from the upstream sources, so simply deleting a record would only
remove it until the next run re-fetched it. Names that have been withheld are
held on a suppression list that every page is generated through, which means a
request applies retroactively to everything already collected and cannot be
undone by a later ingest or by restoring a backup.

To be precise about what that guarantees: it stops the data being **published**.
Withheld records may still exist in the underlying database. If you want your
data erased from storage as well as withheld from the site, say so in your
request and that will be done separately.

**Recipients.** None. The data is not sold, licensed, shared, transferred, or
provided to any third party. It is published on this website and nowhere else.

**International transfers.** The data is stored in the United States. Because it
is published on a public website, it is by definition accessible worldwide.

## 7. Your rights

If you are in the UK or the EEA you have rights of access, rectification,
erasure, restriction, objection, and portability, and you may complain to your
national supervisory authority. Residents of other jurisdictions may have
comparable rights.

**In practice, here is the commitment that matters:**

> **If you ask to be removed from this site, you will be removed. No
> justification is required, no questions will be asked, and no attempt will be
> made to argue you out of it.**
>
> Email **‹PRIVACY@YOURDOMAIN›** with the name as it appears on the site, or a
> link to the page. Removals are actioned within **one business day** and are
> permanent.

The same address handles corrections. If a record misattributes work to you, or
attributes something to you that is not yours, say so and it will be fixed or
removed — accuracy failures are treated as urgent.

You do not need to prove your identity to request removal of your own name. If
a request concerns someone else's name, we may ask how you are connected to
them, only to avoid removing records at the request of an unrelated party.

## 8. Accuracy, and what this site is not

Records are reproduced from their sources automatically and are **not
independently verified**. Automated matching can make mistakes: two people with
similar names can be merged, and an affiliation can be attached to the wrong
person.

**Nothing on this site is a statement about any individual's abilities,
character, conduct, employment status, or professional standing.** A person
appears here because their name is on a public record, and for no other reason.

## 9. Changes

Material changes will be reflected in the "last updated" date above. Because
this project is early and evolving, that date may change often.

---

<!--
REMOVE BEFORE PUBLISHING — implementation notes

REQUIRED, or this document is not true:

  ▸ ‹PRIVACY@YOURDOMAIN› must be a real address you actually read. A privacy
    notice promising one-business-day removal at an unmonitored address is
    worse than no notice: it is a documented, dated, unkept commitment in
    your own words.

  ▸ Section 6's removal guarantee is now backed by suppress.py and
    suppressions.json, read at render time by build_snapshot.py and
    graph_build.py. Verify with:  python suppress.py --selftest
    When a request arrives:  python suppress.py --add "Name" --note "email
    2026-08-22"  then rebuild and redeploy. The rebuild is what makes it
    take effect — the entry alone changes nothing.

  ▸ Section 3 claims no third-party fonts or scripts. Verify this holds for
    all three generated pages before publishing. If any generator ever adds a
    CDN font, this sentence becomes false.

  ▸ Section 5's claim that no scores apply to individuals is true today —
    metrics.py filters to ORG and TECH. If that ever changes, this notice
    must change with it.

TWO OPEN QUESTIONS FOR THE LAWYER:

  1. Article 27 EU representative — see the callout in section 1.
  2. Whether the Article 85(2) journalism/research exemption is available.
     It is member-state law across 27 jurisdictions. If it is available it
     changes this analysis substantially; if it is not, section 5's
     legitimate-interests basis is doing all the work alone.
-->
