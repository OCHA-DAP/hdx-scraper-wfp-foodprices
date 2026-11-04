# 0002: Generate global and HAPI prices files in a single pass with the csv module

## Status

Accepted — 2026-11-04

## Context

To avoid running out of memory, the world run reread every country prices file once per year it
covered (f8b9da8, "Iterate over files repeatedly so as not to run out of memory"). It read with
`get_tabular_rows`, parsed every date with `parse_date` and wrote with `save_iterable`. Both
helpers go through frictionless, and `save_iterable` builds the whole year's row list in memory.

Measured on a full country run's output (75 files, 3.35M rows, 37 years):

| Step | Before | After |
|---|---|---|
| Global year files (year scan + `create_prices_files`) | 81s + 1,898s, 574 MB peak | 14s, 100 MB peak |
| HAPI year files (10 years) | 116s, 703 MB peak | 23s, 151 MB peak |

Peak memory is for the whole process. HAPI peaks include about 140 MB of admin p-code data.

## Decision

- `GlobalPricesGenerator.create_prices_files` reads each country file once with `csv.DictReader`.
  It writes each row to an open `csv.DictWriter` for its year, and returns the earliest date,
  latest date and year to filepath. The separate year scan is removed.
- Dates are YYYY-MM-DD, so the year is `date[:4]` and the date range comes from string comparison.
- `HAPIOutput.create_prices_files` also uses `csv` reading and writing. Each market's warning and
  error strings are built once rather than `deepcopy`-ing the market's base row per price row. The
  reference period is cached per date string.

## Consequences

- Output is byte-identical to the previous code on the full data.
- Memory no longer grows with the size of a year's prices.
- The global and HAPI steps no longer need a `Download` object, so it was removed from the
  `GlobalPricesGenerator` and `HAPIOutput` constructors.
- Merging the country and world runs back into one step is now feasible memory-wise, but it was
  not done. The time saved would only be a container start, and two steps let the world run be
  rerun alone.
