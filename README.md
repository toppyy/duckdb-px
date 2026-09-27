# DuckDB-Px

This DuckDB extension allows you to read [Px-files](https://www.scb.se/en/services/statistical-programs-for-px-files/px-file-format/). `.px` is file format used to publish and distribute (official) statistics. For example, the statistical offices of Sweden, Denmark and Finland distribute data in this format (among others).

## Installing

```sql
INSTALL px FROM community;
LOAD px;
```

## Usage

After installing, you can write arbitrary SQL-queries over px-files with the `read_px` function:
```sql
SELECT * FROM read_px('your_dataset.px');
```

## Functions

The extension registers two table functions. Both of them take the path of a single PX
file as their only argument, neither a glob nor a list of files is accepted.

### `read_px(file VARCHAR)`

Reads a PX file and returns one row per observation:

| Column | Type | Description |
| --- | --- | --- |
| one column per variable | `VARCHAR` | All variables defined with the `STUB` or `HEADING` keyword, named after the keyword and holding the `CODE` of the observation |
| `value` | `INTEGER` or `FLOAT` | The observation under the `DATA` keyword |

The type of the `value` column is a guess based on the `DECIMALS` keyword:

- if `DECIMALS = 0`, the type is `INTEGER`
- if `DECIMALS > 0`, the type is `FLOAT`

```sql
SELECT Vuosi, Sukupuoli, SUM(value) AS population
FROM read_px('your_dataset.px')
WHERE Vuosi = '1900'
GROUP BY ALL;
```

The columns of a file, and their types, are only known after the file has been read:

```sql
DESCRIBE SELECT * FROM read_px('your_dataset.px');
```

### `read_px_metadata(file VARCHAR)`

Reads the `CODE` and `VALUES` entries of every variable without reading the `DATA`
section, which keeps it cheap on large files:

| Column | Type | Description |
| --- | --- | --- |
| `variable` | `VARCHAR` | The variable the entry belongs to, named after its `STUB` or `HEADING` keyword |
| `code` | `VARCHAR` | A `CODE` of the variable |
| `value` | `VARCHAR` | The `VALUES` entry that belongs to the code, `NULL` if the variable has none |

```sql
SELECT * FROM read_px_metadata('your_dataset.px');
```

The codes in the data are the labels of the statistical office in disguise, joining the
metadata to the data translates them back. Every variable needs its own join:

```sql
SELECT vuosi, m.value AS sukupuoli, SUM(p.value) AS population
FROM read_px('statfin_vaerak_pxt_11rc.px') AS p
JOIN read_px_metadata('statfin_vaerak_pxt_11rc.px') AS m
	ON m.variable = 'Sukupuoli' AND m.code = p.Sukupuoli
WHERE vuosi = '1900'
GROUP BY ALL;
```

Both functions reject a malformed file when they are bound, for example when the number
of `CODES` and `VALUES` entries of a variable do not match, or when a variable has no
`CODES` at all.

## On predicate pushdown

Due to the physical layout of the PX file format, only predicates on the first column of
the table are pushed down. The observations of the other variables are interleaved in
the file, so the only way to skip them is to decode them first, which DuckDB's
vectorized and parallelized filter does more efficiently.

The observations of the first variable are laid out sequentially by code: every code
occupies one contiguous block of records. That lets the scan skip the blocks of the
codes that can not match the filter, and stop reading as soon as every code that can
match has been read. When a query filters on the first column more than once, only the
codes that match all of the filters are read. A pushed down filter never changes the
result of a query, because DuckDB keeps applying the filter to the observations that the
scan returns.

Pushed down:

```sql
WHERE vuosi = '2000'                    -- Vuosi = '2000'
WHERE vuosi IN ('1900', '2000')         -- Vuosi IN ('1900', '2000')
WHERE vuosi = '1900' OR vuosi = '2000'  -- Vuosi IN ('1900', '2000')
```

Not pushed down, in which case DuckDB filters the observations itself:

```sql
WHERE vuosi >= '1900'     -- only equality and IN are resolved into codes
WHERE sukupuoli = 'SSS'   -- not the first column
WHERE value = 1           -- the value column has no codes
WHERE vuosi = 1900        -- the binder casts the column instead of the constant
```

You can observe which filters were pushed down in the query plan:

```sql
EXPLAIN SELECT * FROM read_px('statfin_vaerak_pxt_11rc.px') WHERE vuosi = '2000';
```

```
    ┌───────────────────────────┐
    │          READ_PX          │
    │    ────────────────────   │
    │     Function: READ_PX     │
    │                           │
    │       File Filters:       │
    │       Vuosi = '2000'      │
    │                           │
    │           ~1 row          │
    └───────────────────────────┘
```

## Building

### Build steps
Now to build the extension, run:
```sh
make
```
## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

----

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.
