# pg-cdt
Exploring postgres custom data types


This repository compares JSONB and User Defined Type in Postgres to understand and find out:
1. Common operators of JSONB.
2. Use-cases when JSONB operators make use of an index and when they do not.
3. Which one is easier for querying and indexing.
4. Indexing on column of User Defined Type using default operator classes of postgres.

The repo is specific for Postgres with [pgjdbc-ng](https://github.com/impossibl/pgjdbc-ng/) driver, [norm](https://github.com/medly/norm) and code examples are in Kotlin.
