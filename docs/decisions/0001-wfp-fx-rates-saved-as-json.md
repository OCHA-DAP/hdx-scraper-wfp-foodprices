# 0001: Save WFP FX rates as JSON rather than YAML

## Status

Accepted — 2026-11-04

## Context

The country run saves the WFP historic FX rates (707,927 rates across 128 currencies, about 10
minutes to fetch) to its temp folder, so a resumed run (`WHERETOSTART`) can load them instead of
refetching. They were saved as `wfp_rates.yaml`. `load_yaml` uses ruamel's round-trip loader, which
builds a `CommentedMap` and `ScalarFloat` per entry. Measured on a synthetic file of the same size
(18 MB):

| | Time | Peak memory | Held after loading |
|---|---|---|---|
| `load_yaml` | 38s | 1,594 MB | 1,464 MB |
| `json.load`, keys converted to int | 0.2s | 117 MB | ~115 MB |

With IDN's prices (about 0.7 GB at peak), a resumed run reached about 2.2 GB and was killed on
Jenkins (exit code 137).

## Decision

Save and load the rates as `wfp_rates.json` (`setup_currency` in `utilities.py`). JSON object keys
are strings, so timestamps are converted back to int on load.

## Consequences

- A resumed run holds about 115 MB for the rates instead of 1.46 GB.
- The test fixture is `tests/fixtures/input/wfp_rates.json`.
- An existing `wfp_rates.yaml` from a run started before this change is ignored, and the rates are
  refetched once.
