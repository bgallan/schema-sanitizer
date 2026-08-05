# CSV header modes

`csv_header_mode` controls how a conversion reconciles headers when one logical
input contains several CSV sources.

## Index

- [Exact mode](#exact-mode)
- [Union mode contract](#union-mode-contract)

## [Exact mode](#index)

`csv_header_mode="exact"` is the default and preserves the historical reader
behavior. Every source must remain compatible with the canonical header chosen
by the existing CSV implementation. No source projection or column union is
introduced by this option.

## [Union mode contract](#index)

`csv_header_mode="union"` pre-reads every physical CSV header, builds an
immutable projection for each source index, infers the canonical schema from
the complete header union, and selects the matching projection for every row.
The implementation has this contract:

- canonical column order follows the configured field naming and ordering
  policies;
- missing fields are emitted as nulls;
- different physical column orders are accepted;
- duplicate fields within one source header are rejected;
- sources with headers cannot be mixed with sources without headers;
- rows shorter than their source header are padded with nulls;
- rows longer than their source header are rejected;
- header-declared columns that are null in every row remain nullable strings;
- strict schema mode rejects unexpected columns, while additive mode accepts
  them.

The projection metadata is immutable and shared by the grouped frontend. CSV
cell decoding may run concurrently, while projection selection and ordered row
commit remain deterministic. The metadata footprint participates in the
operation memory limit.
