# spqrguard

spqrguard is a PostgreSQL extension that attaches to the executor and blocks writes to SPQR managed relations when they are meant to be read-only. It reads SPQR metadata to identify distributed and reference tables, then raises an error before any modification is executed.

## How it works
- On load (via `shared_preload_libraries`), the extension installs an `ExecutorRun` hook.
- Each statement’s plan is scanned; if the target relation is recorded in `spqr_metadata.spqr_distributed_relations` or `spqr_metadata.spqr_reference_relations`, spqrguard consults its configuration to decide whether writes should be denied.
- Global toggles can be stored in `spqr_metadata.spqr_global_settings`; per-session overrides are available via GUCs.

## Build & Install
```sh
make                # compile
make install        # install extension into the target PostgreSQL instance
```
Reload PostgreSQL after adding `spqrguard` to `shared_preload_libraries`.

## Configuration
- `spqrguard.prevent_distributed_table_modify` (bool, superuser) — block writes to SPQR distributed relations.
- `spqrguard.prevent_reference_table_modify` (bool, superuser) — block writes to SPQR reference relations.
- Global defaults can be stored in `spqr_metadata.spqr_global_settings` with integer keys `42` (distributed) and `69` (reference); string values like `on/yes/true/ok` enable the block.

## Quick usage example
```sql
CREATE EXTENSION spqrguard;
SELECT spqr_metadata.mark_distributed_relation('orders');
SET spqrguard.prevent_distributed_table_modify TO 'on';
INSERT INTO orders VALUES (1);  -- ERROR: unable to modify distributed relation within read-only transaction
```
