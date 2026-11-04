# 0002: Map WFP admin levels to COD levels per country

## Status

Accepted — 2025-05-02 (when the world config was created in #17; the lists may be older)

## Context

For some countries, WFP's admin levels differ from the COD levels that HAPI's p-codes use:

| Country | WFP data | COD level it matches |
|---|---|---|
| CIV, KEN | Admin 1 names are in WFP's admin 2 field | Admin 1 |
| DOM, RUS, UGA | Admin 2 names are in WFP's admin 1 field | Admin 2 |
| SLV | Only admin 1 is matched. The reason was not recorded. | Admin 1 |

## Decision

The world config lists these countries (`unused_adm1`, `unused_adm2` and `adm1_only` in
`world/config/project_configuration.yaml`), and `HAPIOutput.complete_admin` handles them:

- `unused_adm1`: WFP's admin 2 name is looked up as admin 1, and the row is admin level 1.
- `unused_adm2`: WFP's admin 1 name is looked up as admin 2, admin 1 comes from that p-code's
  parent, and the row is admin level 2.
- `adm1_only`: the admin 2 name is not looked up.

## Consequences

- Adding a country with the same mismatch is a config change only.
- The provider's own names are kept in `provider_admin1_name` and `provider_admin2_name`, so the
  original WFP values stay visible in HAPI.
