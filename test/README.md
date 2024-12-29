# Testing

This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, and that is the goal of this extension too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or just the tests with out a release build
```bash
make test_only
```

The px-files used in testing are courtesy of [Statistics Finland](https://stat.fi/index_en.html) and [Statistics Sweden](https://www.scb.se/en)