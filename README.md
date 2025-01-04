# DuckDB-Px

This DuckDB extension allows you read [Px-files](https://www.scb.se/en/services/statistical-programs-for-px-files/px-file-format/). `.px` is file format used to publish and distribute (official) statistics. For example, the statistical offices of sweden, denmark and finland distribute data in this format (among others).

## Usage

After a build/install, you can write arbitrary SQL-queries over px-files with the `read_px` function:
```sql
SELECT * FROM read_px('your_dataset.px');
```
## Schema 

The function returns all variables defined with keywords `STUB` or `HEADING` as varchar-columns. The actual values for the these columns are derived from associated `CODE`-keyword definitions. 

Data under the `DATA`-keyword are represented in a single column called `value` which is an integer or a decimal.

The type of `value`-field is a guess based on the `DECIMALS`-keyword.

- If DECIMALS = 0, the type is INTEGER
- If DECIMALS > 0, the type is DECIMAL(17,*x*) where *x* is the value of DECIMALS

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

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL px
LOAD px
```

----

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.