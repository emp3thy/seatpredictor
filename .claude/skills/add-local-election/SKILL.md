---
name: add-local-election
description: Use when the user wants to add a new UK local-election Projected National Share (PNS) entry to data/hand_curated/local_elections.yaml. Triggers on phrases like "add local election", "new PNS entry", "May elections came in", "update local elections". Walks through sourcing PNS from BBC, Sky, Britain Elects, and Wikipedia, reconciling across sources, and appending a validated entry to the YAML.
---

# /add-local-election — append a new PNS entry to local_elections.yaml

Adds a new event to `data/hand_curated/local_elections.yaml` for the most-recent UK local elections.

## Steps

1. **Determine the date.** If the user named one (e.g. "May 2026"), use the first Thursday of that month. Otherwise ask. Confirm before proceeding.

2. **Look up PNS values from these sources, in order. Use WebFetch on each.** Record each source's per-party shares plus the URL where they were published.

   | Priority | Source | Where to look |
   |---|---|---|
   | 1 | BBC News | `https://www.bbc.co.uk/news/topics/cn4x6dw8430t` (local elections topic) — find the year's live results page; PNS is in the headline summary or "Projected national vote share" section. |
   | 2 | Sky News | `https://news.sky.com/topic/general-election-7457` (or local-election equivalent for the year) — Sky typically publishes a PNS chart on results day. |
   | 3 | Britain Elects | `https://britainelects.com/` — published spreadsheet linked from their results post; usually the most analytically careful PNS calculation. |
   | 4 | Wikipedia | `https://en.wikipedia.org/wiki/<YEAR>_United_Kingdom_local_elections` — PNS table at the top of the article. |

   For each source, extract per-party shares (con, lab, ld, reform, green, plaid, snp, other). Use 0.0 for parties not listed. Lower-case party keys per `PartyCode.value`.

3. **Reconcile across sources:**
   - If 2+ sources published, the consolidated `shares` is the per-party median across sources. Set `consolidated.method = "median_across_sources"`.
   - If only 1 source published (e.g. BBC alone), use it directly. Set `consolidated.method = "sole_source"`.
   - If sources disagree on Reform by more than 2pp, flag in the YAML's `notes` field and surface this to the user before writing — ask whether they want to proceed.

4. **Append the new event to `data/hand_curated/local_elections.yaml`.** Preserve YAML structure:

   ```yaml
   events:
     # ... existing events ...
     - date: <YYYY-MM-DD>
       name: <descriptive name, e.g. "May 2026 London borough and met district elections">
       pns:
         sources:
           - source: "BBC News"
             source_url: "<full URL where the BBC PNS was published>"
             shares: { con: ..., lab: ..., ld: ..., reform: ..., green: ..., plaid: ..., snp: ..., other: ... }
           - source: "Sky News"
             source_url: "<URL>"
             shares: { ... }
         consolidated:
           method: "median_across_sources"   # or "sole_source"
           shares: { con: ..., lab: ..., ld: ..., reform: ..., green: ..., plaid: ..., snp: ..., other: ... }
       notes: <one-line note, e.g. "BBC and Sky agree within 1pp; Reform PNS dominated by gains in mets.">
   ```

5. **Validate the YAML loads** by running:

   ```bash
   .venv/Scripts/python.exe -c "from data_engine.sources.local_elections import load_local_elections; from pathlib import Path; events = load_local_elections(Path('data/hand_curated/local_elections.yaml')); print(f'OK: {len(events)} events loaded')"
   ```

   Expected: `OK: <N> events loaded` with N being the new total. If any warning is emitted (e.g. shares don't sum to 100 ± 2), inspect the YAML and correct.

6. **Tell the user the entry was added.** Suggest re-running notebook 05 to refresh `data/derived/reform_polling_bias.json`:

   > Added <event name>. To refresh the bias artifact, re-run the last cell of notebooks/05_reform_polling_bias.ipynb (or run `nbconvert` on it from inside notebooks/).

## Pitfalls

- **PNS publication is not instant.** BBC usually publishes PNS the morning after polling day; Sky later that day; Britain Elects 1-3 days after; Wikipedia within a week. If you're invoking this on the day-of, only the BBC value may be available — use it as `sole_source` and add a note flagging that the Britain Elects value should be added later.
- **Reform's PNS may include "Reform UK" + "Reform UK aligned independents"** in some sources but not others. Accept the source's published value; do not adjust.
- **Northern Irish parties don't appear in PNS.** GB-only metric. Roll any NI residual into `other`.
- **Don't re-curate existing events without explicit user request.** This skill appends only.
