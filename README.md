# oddie

ODBC SQL Query daemon

oddie - run SQL queries, command line ODBC access

Can do single queries or run in "daemon" mode.

### Usage: `oddie DRVC [SQL]`

Where `DRVC` is the ODBC driver connection string and can specify a:
```
registered DSN: "DSN=registered_dsn_name;uid=myusername;pwd=mypassword"
or
driver: "Driver={Microsoft Access Driver (*.mdb)};DBQ=myfile.mdb"
```

`SQL` is optional; a valid SQL statement.

If no SQL statement is provided, oddie enters daemon mode and accepts properly formatted requests from STDIN and provides formatted responses to STDOUT.

### Format of input:

`SQL="any valid select/insert/update/delete",ID="correlation_id",MD5="_MD5SUM_OF_PREVIOUS_RESULTS_",ZIP=[0-9];`

To list all tables, views, and other objects in the data source, send `TABLES=1` instead of a SQL statement:

`TABLES=1,ID="optional_correlation_id",MD5="optional_previous_md5",ZIP=[0-9];`

The response is a result set with five columns: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `REMARKS`. Use the `TABLE_TYPE` column to filter by `TABLE`, `VIEW`, etc.

`ID`, `MD5`, and `ZIP` are all optional.

`ID` is an opaque string echoed back in the response for call correlation.

`MD5` and `ZIP` are only relevant for SELECT queries. If specified:

For `MD5`, if the MD5 of the query results matches the submitted value, return `MD5=HASH,RESULT=CACHED;` instead of the full result set. If not specified, or not equal, return the complete result set.

For `ZIP`, compress results to the level specified (0 = none, 1–9 = zlib compression; useful for very large result sets).

### Format of output:

On error: `ERROR="encoded error message(s) as reported by ODBC";`

On insert, update, delete: `ROWCOUNT=num_of_rows_affected;`

When a SELECT MD5 value matches: `MD5=FF1519FFFFFFFFFFFFFFFF115B15FFFF,RESULT=CACHED;`

When a SELECT has no results: `RESULT="";`

When returning SELECT results: `RESULT="encoded output of header and rows",MD5=HASH;` or with compression: `RESULT="compressed_encoded_data",MD5=HASH,ZIP=level;`

When an `ID` was supplied in the request, it is echoed at the start of the response: `ID="value",RESULT=...;`

For RESULT and ERROR, encoding is:

Each field has certain characters (eg `\n`, `\t`, `=`, `;`, etc.) hex-encoded (eg `\t` becomes `%09`, `\n` become `%0a`).

Additionally, for RESULT:

Record separator: `\n`

Field separator: `\t`

The semi-colon is a terminator and is always required. After a terminator is received, it parses the string, runs the query, and returns the results, also terminated with a semi-colon.

### In daemon mode:

After init, oddie outputs `OK` to STDIO and then listens to STDIN for commands of the above format.

To terminate, send `CLOSE=0;` provides a clean shutdown but is optional.

### Dependencies:

MD5 implementation (included)

Zlib (fetched automatically by `build.sh`)

### To build (cross-compile on Linux for Windows):
```
./build.sh
```

Requires `curl`, `tar`, and a MinGW cross-compiler (`i686-w64-mingw32-gcc`, `i686-pc-mingw32-gcc`, or `mingw32-gcc`). The script checks [zlib.net](https://zlib.net) for a newer zlib version and downloads it if available.
