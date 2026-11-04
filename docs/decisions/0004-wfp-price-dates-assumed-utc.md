# 0004: Treat WFP price dates without a timezone as UTC

## Status

Accepted — 2026-07-27

## Context

WFP's price dates have no timezone. Before the move to the DataBridges client (#30), they were
parsed with `parse_date`, which returns UTC datetimes. `data_bridges_client` returns naive
datetimes instead, so comparing them with the UTC defaults (`default_date`, `default_enddate`)
fails.

## Decision

`WFPFood.generate_rows` sets UTC on any naive `commodity_price_date`.

## Consequences

- Output dates and dataset date ranges are unchanged from the `parse_date` behaviour.
- If WFP starts returning dates with a timezone, they are used as given.
