# 0003: Map WFP admin names explicitly where AdminLevel's matcher fails

## Status

Accepted — 2026-10-14

## Context

The world run matches WFP's admin names to COD p-codes with hdx-python-country's `AdminLevel`.
Moving to `RapidFuzzMatcher` (hdx-python-country 4.2.2) changed admin codes for 997 HAPI markets
compared with prod, many of them wrongly (#31, #32). Examples:

- YEM "Al Hudaydah" matched Al Bayda (YE14) instead of Al Hodeidah (YE18).
- CMR "Ouest" matched East (CM003) instead of West (CM008).
- Some names are transliterations or old names, e.g. VEN "Vargas", renamed La Guaira.
- Some regions were split, e.g. GHA Brong Ahafo into Ahafo, Bono and Bono East, so no single
  admin 1 code is correct.

## Decision

Pass `admin1_config` and `admin2_config` from `world/config/project_configuration.yaml` to each
`AdminLevel`:

- `admin_name_mappings` pins `"{iso3}|{WFP name}"` to the correct p-code. Each entry's comment gives
  the COD name and, where the matcher picked something else, the wrong match.
- `admin_fuzzy_dont` lists names that must not be fuzzy matched: names with no COD equivalent and
  split regions, where the admin 2 name decides the admin 1 instead.
- Admin 1 and admin 2 have separate configs, because a mapped p-code must be at the level being
  looked up. For countries in `unused_adm1` (see 0002), the admin 1 keys are WFP's admin 2 names.
- When admin 1 is blank and admin 2 matches, admin 1 is taken from the admin 2 p-code's parent.

## Consequences

- `tests/test_admin_matching.py` scores 393 markets whose codes changed. With the mappings, admin 1
  is 393/393 and admin 2 380/380 (13 not scored). The mappings were derived from this set, so it
  confirms they resolve as intended rather than measuring matcher accuracy.
- Each new mismatch needs a new mapping entry.
