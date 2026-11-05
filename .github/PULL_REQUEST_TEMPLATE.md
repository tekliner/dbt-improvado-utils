<!--
PR template for dbt-improvado-utils, modelled on tekliner/tcs-chrome-extension.
The package is pinned by tag in dbt_improvado_clickhouse, dbt_customer (+ attribution/) and
dbt-internal-analytics-sources: a merge changes nothing in production until a consumer bumps the tag.
-->

## Summary

<!-- 1-3 sentences: what changes and why. Link the ticket. -->

## Risk classification

- [ ] **Risk:** low / medium / high
- [ ] **What it touches:** a materialization (`microbatch`, `incremental_and_live`, `table_by_parts`, `mv`, `custom_dictionary`, `create_custom`) / a macro / tests or CI only
- [ ] **Default behaviour changes for models that don't set the new config?** no / yes — explain
- [ ] **Reversible?** yes (pin the previous tag) / no (it rewrites or drops data in target tables)

## Consumers

- [ ] Tag to release after merge: `v1.3.__`
- [ ] Who bumps the tag and where: dbt_improvado_clickhouse / dbt_customer / dbt-internal-analytics-sources
- [ ] Models affected in the first consumer: `<list or selector>`

## Test plan

- [ ] pytest: `tests/pytest/<path>::<test>` — fails without this change
- [ ] Run in a consumer (CI build or local run on a pinned branch/commit): `<link>`
- [ ] N/A — explain why

## Rollback plan

<!-- Normally: pin the previous tag in the consumer's packages.yml. Say what else is needed if
the change already rewrote data (e.g. a partition rebuild). -->

## Out of scope (deferred)

<!-- Anything intentionally left for a follow-up — link the ticket. -->
