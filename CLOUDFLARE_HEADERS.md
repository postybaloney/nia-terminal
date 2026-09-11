# Security headers via Cloudflare — audit finding 07

The site scored **F** on an external header scan. GitHub Pages cannot set
response headers, but Cloudflare already sits in front of `parthrudesai.com`,
so all four headers below are dashboard configuration rather than a code
change.

Every value here was **tested against the actual generated pages** — all six
were served with these exact headers and checked for CSP violations, JavaScript
errors, and whether the page still functioned (not merely loaded). Zero
violations, zero errors, graph still interactive.

---

## The four headers

Add these as four separate Response Header Transform Rules.

### 1 · Content-Security-Policy

```
default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; font-src 'none'; connect-src 'none'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'; upgrade-insecure-requests
```

This is far tighter than a typical CSP, and it is only possible because the
pages turned out to be genuinely self-contained. Measured across all six:

| | index | graph | issue | privacy | terms | sources |
|---|---|---|---|---|---|---|
| External scripts / stylesheets / images | 0 | 0 | 0 | 0 | 0 | 0 |
| `fetch` / XHR / WebSocket / beacon | 0 | 0 | 0 | 0 | 0 | 0 |
| Inline `on*` event attributes | 0 | 0 | 0 | 0 | 0 | 0 |
| Inline `<style>` | 2 | 2 | 2 | 1 | 1 | 1 |
| Inline `<script>` | 0 | 1 | 0 | 0 | 0 | 0 |

**`connect-src 'none'` is the directive that earns its place.** The pages make
no network requests of any kind, so forbidding them costs nothing — and it
means that even if a fourth injection hole opens up, injected JavaScript has
nowhere to send what it steals. Exfiltration is the actual harm; this blocks it
at the browser.

**`'unsafe-inline'` is the compromise, and it is worth naming.** It is required
because `graph.html` is deliberately self-contained: its node payload and all
its logic are inline. So this CSP does **not** stop an injected inline
`<script>` from running. It stops that script loading anything external,
calling home, hijacking `<base>`, submitting a form, or being framed.

To remove `'unsafe-inline'` you would externalise `graph.html`'s script and
payload into separate files and switch to `script-src 'self'`. That is a real
improvement and it costs the self-contained property — the graph would stop
being a single file you can email to someone. Worth doing eventually, not
worth blocking these headers on.

### 2 · Referrer-Policy

```
strict-origin-when-cross-origin
```

Not just hardening — a privacy fix. Without it every outbound source link
sends the full referring URL to a third party, including
`graph.html?focus=…&name=…` when someone deep-links into an entity. That is
the site's own structure leaking to Google, the FDA, NIH and every publisher
linked from a record.

### 3 · X-Content-Type-Options

```
nosniff
```

Free. Stops a browser second-guessing a declared content type.

### 4 · Permissions-Policy

```
geolocation=(), camera=(), microphone=(), payment=(), usb=(), interest-cohort=()
```

The site uses none of these. Denying them all costs nothing and removes the
possibility of an injected script asking for them.

**`X-Frame-Options` is deliberately omitted** — `frame-ancestors 'none'` in the
CSP supersedes it, and the audit rated clickjacking as meaningless against a
read-only page anyway.

---

## Steps

1. Cloudflare dashboard → select the `parthrudesai.com` zone
2. **Rules** → **Overview**
3. **Create rule** → **Response Header Transform Rule**
4. **Rule name**: e.g. `CSP — NIA terminal`
5. Under **When incoming requests match**, scope it. Either apply to all
   incoming requests, or — safer while testing — limit it to the terminal:
   `URI Path` `starts with` `/nia-terminal/`
6. Under **Modify response header**, choose
   **Set static — Sets the value of an HTTP response header to a static string value**
7. **Header name** and **Value** from the table above
8. **Deploy**
9. Repeat for the other three headers

> ⚠ **Use "Set static", not "Add".** Cloudflare's own client-side security
> feature also adds CSP headers, and when a browser receives two CSP headers it
> **enforces both, with the most restrictive winning** — which produces
> resources being blocked for reasons that appear nowhere in your rule. *Set
> static* replaces rather than appends, so your rule is the only CSP present.
> This is Cloudflare's documented guidance, and it is the single most likely
> way to get this wrong.

---

## Verifying

Re-run the scan that produced the F:

    https://securityheaders.com/?q=https%3A%2F%2Fparthrudesai.com%2Fnia-terminal%2F

Then open `graph.html`, click a node, and confirm the detail panel still
populates and the source links still open. A CSP that breaks the graph is worse
than no CSP, because it fails silently — the console shows the violation and
the page just looks empty.

If a rule does not seem to fire, Cloudflare Trace
(<https://developers.cloudflare.com/rules/trace-request/>) shows which rules
matched a given URL.

---

## Scope note

Setting these at the zone level applies them to **all of `parthrudesai.com`**,
not only the terminal. That is fine for headers 2–4. It is **not** fine for the
CSP: the main site is a different codebase with different resources, and
`default-src 'none'` would very likely break it. Scope the CSP rule to
`/nia-terminal/` (step 5) unless you have separately tested the main site
against it.

---

*Written 9 September 2026. Header values verified against the demo build of all
six pages; re-verify if `graph_render.py` ever gains an external dependency.*
