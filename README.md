# oddie
ODBC SQL Query daemon
Copyright (C) Scott Weisman

oddie - run SQL queries, command line ODBC access
Can do single queries or run in "daemon" mode.

usage: `oddie DRVC [SQL]`
Where DRVC is the ODBC driver connection string and can specify a:
    registered DSN: "DSN=registered_dsn_name;uid=myusername;pwd=mypassword"
    or
    driver: "Driver={Microsoft Access Driver (*.mdb)};DBQ=myfile.mdb"

SQL is optional; a valid SQL statement.

If no SQL statement is provided, oddie enters daemon mode and accepts properly formatted requests from STDIN and provides formatted responses to STDOUT.

Format of input:
SQL="any valid select/insert/update/delete",MD5="_MD5SUM_OF_PREVIOUS_RESULTS_",ZIP=[0-9];

MD5 and ZIP are optional, and only relevant for SELECT queries. If specified:

For MD5, if the MD5 of the query results is equal to what is submitted, return result: `xxx`. If not specified, or not equal, return complete result set.
For ZIP, compress results to the level specified (useful for very large result sets).

Format of output:
On error: `ERROR="encoded error message(s) as reported by ODBC";`
On insert, update, delete: `ROWCOUNT=num_of_rows_affected;`
When a SELECT MD5 value matches: `MD5=FF1519FFFFFFFFFFFFFFFF115B15FFFF,RESULT=CACHED;`
When a SELECT has no results: `RESULT="";`
When returning SELECT results: `RESULT="encoded output of header and rows",MD5=XXX;`

For RESULT and ERROR, encoding is:
Each field has certain characters (eg `\n`, `\t`, `=`, `;`, etc.) hex-encoded.
Additionally, for RESULT:
Record separator: `\n`
Field separator: `\t`

The semi-colon is a terminator and is always required. After a terminator is received, it parses the string, runs the query, and returns the results, also terminated with a semi-colon.

In daemon mode:
After init, oddies outputs `OK` to STDIO and then listens to STDIN for commands of the above format.
To terminate, send `CLOSE=0;` provides a clean shutdown but is optional.

Dependencies:
MD5 implementation (included)
Zlib https://www.zlib.net/ (download latest and extract `*.c` and `*.h` files to the repo directory)

To build using MingW in Linux:
```
i686-w64-mingw32-gcc -Wall -Wextra -pedantic -std=gnu99 -Werror -Os -s -static -I /opt/cmf/src/oddie oddie.c md5.c compress.c deflate.c crc32.c adler32.c trees.c zutil.c -o oddie.exe -lodbc32 -Wl,-verbose,--subsystem,console
```
