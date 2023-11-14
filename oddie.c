/*
 * Copyright (C) Scott Weisman
 */

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#if defined(WIN32)
#  include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include "md5.h"
#include "zlib.h"

#if defined(WIN32) || defined(__CYGWIN__)
#  include <fcntl.h>
#  include <io.h>
#  define SET_BINARY_MODE(file) _setmode(fileno(file), O_BINARY)
#else
#  define SET_BINARY_MODE(file)
#endif
#if defined(__MWERKS__) && __dest_os != __be_os && __dest_os != __win32_os
#  include <unix.h> /* for fileno */
#endif
#ifndef WIN32 /* unlink already in stdio.h for WIN32 */
  extern int unlink OF((const char *));
#endif

#define QUERY_BUFFER_SIZE 8192
#define Z_CHUNK (256 * 1024)

#define IS_SQL_SUCCESS(x) ((x) == SQL_SUCCESS || (x) == SQL_SUCCESS_WITH_INFO)
#define hex_digit_to_int(c) \
    ((c) >= '0' && (c) <= '9' ? (c) - '0' : \
        ((c) >= 'a' && (c) <= 'f' ? (c) - 'a' + 10 : \
            ((c) >= 'A' && (c) <= 'F' ? (c) - 'A' + 10 : \
                0)))

typedef struct
{
   SQLSMALLINT TargetType;
   SQLPOINTER TargetValuePtr;
   SQLINTEGER BufferLength;
   SQLLEN StrLen_or_Ind;
} DataBinding;

typedef struct
{
    SQLCHAR       col_name[64];
    SQLSMALLINT   col_name_len;
    SQLSMALLINT   data_type;
    SQLUINTEGER   col_size;
    SQLSMALLINT   decimal_digits;
    SQLSMALLINT   nullable;
    SQLINTEGER    io_len;
} s_col_data;

typedef struct
{
    char    id[64];
    char    md5[33];
    char    sql[QUERY_BUFFER_SIZE];
    int     zip;
} s_request;

char *field_sep = "\t", *rec_sep = "\n";

void sql_fetch(SQLHSTMT sth, SQLSMALLINT col_count, FILE *stream, char *md5, unsigned long *total_len);
int get_request(s_request *request);
void temp_file_name(char *tmpnam);
int error(char *src, RETCODE rv, SQLSMALLINT htype, SQLHANDLE h);
char *url_encode(const char *src, int len, int force, char *buffer);
long encode_out(FILE *stream, unsigned char *b, long len);
int oddie_deflate(FILE *source, FILE *dest, int level);
void cleanup(SQLHENV henv, SQLHDBC dbh, SQLHSTMT sth);

int main(int argc, char *argv[])
{
    RETCODE       rv;
    SQLSMALLINT   col_count, i;
    SQLINTEGER    row_count;
    SQLHENV       henv = SQL_NULL_HENV;
    SQLHDBC       dbh = SQL_NULL_HDBC;
    SQLHSTMT      sth = SQL_NULL_HSTMT;
    FILE          *stream, *zstream;
    s_request     request = {0};
    int           argi = 1;
    unsigned long length;
    unsigned char buffer[1024 + 1], daemon = 0;
    char          md5[33], *query = NULL, *encoded = NULL, filename[MAX_PATH], zfilename[MAX_PATH];

    SET_BINARY_MODE(stdout);

    if (argc < 2 || !argv[argi])
    {
        printf("usage: %s dsn_string [sql]", argv[0]);
        exit(0);
    }
    else if (!argv[argi + 1])
    {
        daemon = 1;
        query = request.sql;
#if defined(WIN32)
        SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
#endif
    }
    else
        query = argv[argi + 1];

    rv = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (error("SQLAllocHandle1", rv, SQL_HANDLE_ENV, henv))
        goto CLEANUP;

    rv = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC2, SQL_IS_INTEGER);
    if (error("SQLSetEnvAttr", rv, SQL_HANDLE_ENV, henv))
        goto CLEANUP;

    rv = SQLSetEnvAttr(henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER) SQL_CP_ONE_PER_DRIVER, SQL_IS_INTEGER);
    if (error("SQLSetEnvAttr", rv, SQL_HANDLE_ENV, henv))
        goto CLEANUP;

    rv = SQLAllocHandle(SQL_HANDLE_DBC, henv, &dbh);
    if (error("SQLAllocHandle2", rv, SQL_HANDLE_ENV, henv) || !dbh)
        goto CLEANUP;

    rv = SQLDriverConnect(dbh, NULL, (SQLCHAR *) argv[argi], SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (error("SQLDriverConnect", rv, SQL_HANDLE_DBC, dbh))
        goto CLEANUP;

    if (daemon)
    {
        fputs("OK", stdout);
        fflush(stdout);
    }

    for (;;)
    {
        if ((daemon && !get_request(&request)) || !query[0])
            break;

        char *sql = query;
        while (sql[0] < 33)
            sql++;
        char sql_type = tolower(sql[0]);

        row_count = col_count = -1;

        rv = SQLAllocHandle(SQL_HANDLE_STMT, dbh, &sth);
        if (error("SQLAllocHandle3", rv, SQL_HANDLE_DBC, dbh) || !sth)
            goto CLEANUP;

        if (sql_type == 't')
        {
            // list tables
            //~ https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function?view=sql-server-ver16

            //~ int numCols = 5;
            //~ DataBinding *catalogResult = (struct DataBinding *) malloc(numCols * sizeof(struct DataBinding));

            //~ // allocate memory for the binding - free this memory when done
            //~ for (i = 0; i < numCols; i++)
            //~ {
                //~ catalogResult[i].TargetType = SQL_C_CHAR;
                //~ catalogResult[i].BufferLength = (1024 + 1);
                //~ catalogResult[i].TargetValuePtr = malloc(sizeof(unsigned char) * catalogResult[i].BufferLength);
            //~ }

            //~ // setup the binding (can be used even if the statement is closed by closeStatementHandle)
            //~ for (i = 0 ; i < numCols ; i++)
                //~ rv = SQLBindCol(sth, (SQLUSMALLINT) i + 1, catalogResult[i].TargetType, catalogResult[i].TargetValuePtr, catalogResult[i].BufferLength, &(catalogResult[i].StrLen_or_Ind));

// output header row
//~ for (i = 1; i <= col_count; i++)
//~ {
    //~ buffer = (unsigned char *) url_encode((char *) col_data[i].col_name, strlen((char *) col_data[i].col_name), 0, NULL);
    //~ fputs((char *) buffer, stream);
    //~ free(buffer);
    //~ if (i < col_count)
        //~ fputs(field_sep, stream);  encode_out(stdout, field_sep, 1);

//~ }

//~ fputs(rec_sep, stream); encode_out(stdout, rec_sep, 1);

//~ fputs("RESULT=\"", stdout);
//~ while ((i = fread(buffer, 1, 1024, stream)))
    //~ encode_out(stdout, buffer, i);
//~ fputs("\";", stdout);

//~ for (;;)
//~ {
    //~ rv = SQLFetch(sth);

    //~ if (IS_SQL_SUCCESS(rv))
    //~ {
        //~ for (i = 1; i <= col_count; i++)
        //~ {
            //~ for (;;)
            //~ {
                //~ rv = SQLGetData(sth, i, col_data[i].data_type, buffer, buffer_size, &copy_len);

                //~ if (IS_SQL_SUCCESS(rv) && copy_len != SQL_NULL_DATA && copy_len != 0)
                //~ {
                    //~ copy_len = ((SQLUINTEGER) copy_len > buffer_size) || (copy_len == SQL_NO_TOTAL) ? (SQLINTEGER) buffer_size : copy_len;
                    //~ *total_len += encode_out(stream, buffer, copy_len);
                    //~ MD5Update(&md5_state, buffer, copy_len);

                    //~ if (rv == SQL_SUCCESS_WITH_INFO && SQLGetDiagField(SQL_HANDLE_STMT, sth, 1, i, &status, SQL_INTEGER, &status_size) != SQL_NO_DATA)
                        //~ continue;
                //~ }

                //~ break;
            //~ }

//~ do {
//~ rv = SQLGetData(sth, i, col_data[i].data_type, buffer, buffer_size, &copy_len);
//~ } while (rv == SQL_SUCCESS_WITH_INFO && SQLGetDiagField(SQL_HANDLE_STMT, sth, 1, i, &status, SQL_INTEGER, &statuslen) != SQL_NO_DATA);

            //~ if (i < col_count)
                //~ fputs(field_sep, stream);
        //~ }

        //~ fputs(rec_sep, stream);
    //~ }
    //~ else
        //~ break;
//~ }

            // all catalogs query
            //~ printf("table\n");
            //~ rv = SQLTables(sth, (SQLCHAR *) SQL_ALL_CATALOGS, SQL_NTS, (SQLCHAR *) "", SQL_NTS, (SQLCHAR *) "", SQL_NTS, (SQLCHAR *) "", SQL_NTS);
            //~ for (rv = SQLFetch(sth); IS_SQL_SUCCESS(rv); rv = SQLFetch(sth))
                //~ if (catalogResult[0].StrLen_or_Ind != SQL_NULL_DATA)
                    //~ printf("%s\n", (char *) catalogResult[0].TargetValuePtr);

            //~ for (i = 0; i < numCols; i++)
                //~ free(catalogResult[i].TargetValuePtr);
            //~ free(catalogResult);
        }
        else
        {
            // select/insert/update/delete
            rv = SQLExecDirect(sth, (UCHAR *) sql, SQL_NTS);
            if (error("SQLExecDirect", rv, SQL_HANDLE_STMT, sth))
                goto CLEANUP;

            rv = SQLNumResultCols(sth, &col_count);
            if (error("SQLNumResultCols", rv, SQL_HANDLE_STMT, sth) || col_count < 0)
                goto CLEANUP;

            rv = SQLRowCount(sth, &row_count);
            if (error("SQLRowCount", rv, SQL_HANDLE_STMT, sth))
                goto CLEANUP;

            if (request.id[0])
            {
                encoded = url_encode(request.id, strlen(request.id), 0, NULL);
                printf("ID=\"%s\",", encoded);
                free(encoded);
            }

            if ((sql_type == 'i' || sql_type == 'u' || sql_type == 'd') && row_count > -1)
            {
                // insert/update/delete
                // ODBC specifies SQLRowCount() only returns a value on INSERT/UPDATE/DELETE
                // but MariaDB ODBC connector doesn't adhere to the spec, hence the special case code
                // they thought they were clever. they were, but they were wrong
                // only return ROWCOUNT according to the ODBC spec
                printf("ROWCOUNT=%d;", (int) row_count);
            }
            else if (col_count < 1)
            {
                // select without results
                fputs("RESULT=\"\";", stdout);
            }
            else
            {
                // select with results
                filename[0] = 0;
                temp_file_name(filename);

                stream = fopen(filename, "wb");
                sql_fetch(sth, col_count, stream, md5, &length); // xxx length is total char length of returned data
                fclose(stream);

                printf("MD5=%s,", md5);

                if (request.md5[0] && strcmp(md5, request.md5) == 0)
                {
                    fputs("RESULT=CACHED;", stdout);
                }
                else
                {
                    if (length < 128)
                        request.zip = 0;
                    else if (request.zip)
                        request.zip = (length < 512 ? 5 : 9);

                    if (request.zip)
                    {
                        printf("ZIP=%d,", request.zip);

                        zfilename[0] = 0;
                        temp_file_name(zfilename);

                        stream = fopen(filename, "rb");
                        zstream = fopen(zfilename, "wb");

                        rv = oddie_deflate(stream, zstream, request.zip);

                        fclose(stream);
                        fclose(zstream);

                        stream = fopen(zfilename, "rb");
                    }
                    else
                        stream = fopen(filename, "rb");

                    fputs("RESULT=\"", stdout);

                    while ((i = fread(buffer, 1, 1024, stream)))
                        encode_out(stdout, buffer, i);

                    fputs("\";", stdout);

                    fclose(stream);

                    if (request.zip)
                        DeleteFile(zfilename);
                }

                DeleteFile(filename);
            }

            fflush(stdout);
        }

        SQLFreeHandle(SQL_HANDLE_STMT, sth);
        sth = SQL_NULL_HSTMT;

        if (!daemon)
            break;
    }

    CLEANUP:
    cleanup(henv, dbh, sth);

    return 0;
}

void sql_fetch(SQLHSTMT sth, SQLSMALLINT col_count, FILE *stream, char *md5, unsigned long *total_len)
{
    SQLSMALLINT i, status, status_size;
    SQLRETURN rv;
    SQLINTEGER copy_len;
    unsigned char *buffer;
    MD5Context md5_state;
    s_col_data *col_data = (s_col_data *) malloc((col_count + 1) * sizeof(s_col_data));
    SQLUINTEGER buffer_size = 0;
    unsigned char md5_raw[16];
    unsigned char has_blob = 0;

    // col 0 is the bookmark column
    // get info for each col

    for (i = 1; i <= col_count; i++)
    {
        rv = SQLDescribeCol(sth, i,
                            col_data[i].col_name,
                            sizeof(col_data[i].col_name),
                            &(col_data[i].col_name_len),
                            &(col_data[i].data_type),
                            &(col_data[i].col_size),
                            &(col_data[i].decimal_digits),
                            &(col_data[i].nullable));

        //if (error(rv, SQL_HANDLE_STMT, sth))
        //    log("problem binding column %d name: %s\n", i, col_data[i].col_name);

        if (col_data[i].col_size > buffer_size)
            buffer_size = col_data[i].col_size;

        switch (col_data[i].data_type)
        {
            case SQL_LONGVARCHAR:
                col_data[i].data_type = SQL_CHAR;
                has_blob = 1;
                break;
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                col_data[i].data_type = SQL_C_BINARY;
                has_blob = 1;
                break;
            default:
                col_data[i].data_type = SQL_C_CHAR;
                break;
        }
    }

    // output header row
    for (i = 1; i <= col_count; i++)
    {
        buffer = (unsigned char *) url_encode((char *) col_data[i].col_name, strlen((char *) col_data[i].col_name), 0, NULL);
        fputs((char *) buffer, stream);
        free(buffer);
        if (i < col_count)
            fputs(field_sep, stream);
    }

    fputs(rec_sep, stream);

    *total_len = 0;

    MD5Init(&md5_state);

    // increase column size for binary fields, and because of some misreporting of length
    buffer_size = (buffer_size * 2) + 128;
    if (has_blob && buffer_size < 32768)
        buffer_size = 32768;
    buffer = (unsigned char *) malloc(buffer_size);

    for (;;)
    {
        rv = SQLFetch(sth);

        if (IS_SQL_SUCCESS(rv))
        {
            for (i = 1; i <= col_count; i++)
            {
                for (;;)
                {
                    rv = SQLGetData(sth, i, col_data[i].data_type, buffer, buffer_size, &copy_len);

                    if (IS_SQL_SUCCESS(rv) && copy_len != SQL_NULL_DATA && copy_len != 0)
                    {
                        copy_len = ((SQLUINTEGER) copy_len > buffer_size) || (copy_len == SQL_NO_TOTAL) ? (SQLINTEGER) buffer_size : copy_len;
                        *total_len += encode_out(stream, buffer, copy_len);
                        MD5Update(&md5_state, buffer, copy_len);

                        if (rv == SQL_SUCCESS_WITH_INFO && SQLGetDiagField(SQL_HANDLE_STMT, sth, 1, i, &status, SQL_INTEGER, &status_size) != SQL_NO_DATA)
                            continue;
                    }

                    break;
                }

//~ do {
    //~ rv = SQLGetData(sth, i, col_data[i].data_type, buffer, buffer_size, &copy_len);
//~ } while (rv == SQL_SUCCESS_WITH_INFO && SQLGetDiagField(SQL_HANDLE_STMT, sth, 1, i, &status, SQL_INTEGER, &statuslen) != SQL_NO_DATA);

                if (i < col_count)
                    fputs(field_sep, stream);
            }

            fputs(rec_sep, stream);
        }
        else
            break;
    }

    fflush(stream);
    free(col_data);
    free(buffer);
    MD5Final(md5_raw, &md5_state);
// xxx do i need to free(md5_state)?

    url_encode((char *) md5_raw, 16, 1, md5);
}

#define tID     1
#define tMD5    2
#define tZIP    3
#define tSQL    4

int get_request(s_request *request)
{
    char buffer[8 * 1024];
    unsigned int pos = 0;
    int target = 0;
    char c, hex_hi, hex_low;

    request->zip = request->id[0] = request->md5[0] = request->sql[0] = buffer[0] = 0;

    while (!feof(stdin))
    {
        c = fgetc(stdin);

        if (c == '=')
        {
            buffer[pos] = 0;

            switch (buffer[0])
            {
                case 'I':
                    target = tID;
                    break;
                case 'M':
                    target = tMD5;
                    break;
                case 'Z':
                    target = tZIP;
                    break;
                case 'S':
                    target = tSQL;
                    break;
                default:
                    return 0;
            }

            buffer[pos = 0] = 0;
        }
        else if ((c == ',') || (c == ';'))
        {
            buffer[pos] = 0;

            switch (target)
            {
                case tID:
                    strncpy(request->id, buffer, 64);
                    break;
                case tMD5:
                    strncpy(request->md5, buffer, 33);
                    break;
                case tSQL:
                    strncpy(request->sql, buffer, QUERY_BUFFER_SIZE);
                    break;
                case tZIP:
                    request->zip = atoi(buffer);
                    break;
                default:
                    return 0;
            }

            buffer[pos = 0] = 0;

            if (c == ';')
                return 1;
        }
        else if (c == '"')
        {
            while (!feof(stdin))
            {
                c = fgetc(stdin);

                if (c == '"')
                    break;
                else if (c == '%')
                {
                    hex_hi = fgetc(stdin);
                    hex_low = fgetc(stdin);
                    c = (hex_digit_to_int(hex_hi) << 4) | hex_digit_to_int(hex_low);
                }

                buffer[pos] = c;
                    if (++pos > sizeof(buffer))
                    return 0;
            }

            buffer[pos] = 0;
        }
        else if (isalnum(c))
        {
            buffer[pos] = c;
            if (++pos > sizeof(buffer))
                return 0;
        }
    }

    return 0;
}

long encode_out(FILE *stream, unsigned char *b, long len)
{
    char encode[4];
    long i, n = 0;

    for (i = 0; i < len; i++)
    {
        if (b[i] < 32 || b[i] == '"' || b[i] == '%' || b[i] == ';' || b[i] == ',' || b[i] == '=')
        {
            sprintf(encode, "%%%02X", b[i]);
            n+= fputs(encode, stream);
        }
        else
        {
            fputc(b[i], stream);
            n++;
        }
    }

    return n;
}

char *url_encode(const char *src, int len, int force, char *buffer)
{
    char *dest, encode[3], tmp;
    int j, k;

    if (src == NULL)
        return NULL;

    dest = (buffer ? buffer : (char *) malloc(sizeof(char) * len * 3 + 1));

    for (j = k = 0; j < len; j++)
    {
        tmp = src[j];

        if (force ||
            tmp == '\0' || tmp == '\t' || tmp == '\n' ||
            tmp == '\r' || tmp == '"' || tmp == '%' ||
            tmp == ';' || tmp == ',' || tmp == '=')
        {
            // force is for md5 encoding, convert to hex string without '%'
            if (!force)
                dest[k++] = '%';

            sprintf(encode, "%02X", tmp);
            dest[k++] = encode[0];
            dest[k++] = encode[1];
        }
        else
            dest[k++] = tmp;
    }

    dest[k] = 0;
    return dest;
}

/*
 * def() function copied from zlib zpipe.c renamed to oddie_deflate()
 * Compress from file source to file dest until EOF on source.
 * Returns Z_OK on success,
 * Z_MEM_ERROR if memory could not be allocated for processing,
 * Z_STREAM_ERROR if an invalid compression level is supplied,
 * Z_VERSION_ERROR if the version of zlib.h and the version of the library linked do not match,
 * Z_ERRNO if there is an error reading or writing the files.
 */
int oddie_deflate(FILE *source, FILE *dest, int level)
{
    int ret, flush;
    unsigned have;
    z_stream strm;
    unsigned char in[Z_CHUNK];
    unsigned char out[Z_CHUNK];

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, level);
    if (ret != Z_OK)
        return ret;

    /* compress until end of file */
    do {
        strm.avail_in = fread(in, 1, Z_CHUNK, source);
        if (ferror(source)) {
            (void)deflateEnd(&strm);
            return Z_ERRNO;
        }
        flush = feof(source) ? Z_FINISH : Z_NO_FLUSH;
        strm.next_in = in;

        /* run deflate() on input until output buffer not full, finish
           compression if all of source has been read in */
        do {
            strm.avail_out = Z_CHUNK;
            strm.next_out = out;
            ret = deflate(&strm, flush);    /* no bad return value */
            assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
            have = Z_CHUNK - strm.avail_out;
            if (fwrite(out, 1, have, dest) != have || ferror(dest)) {
                (void)deflateEnd(&strm);
                return Z_ERRNO;
            }
        } while (strm.avail_out == 0);
        assert(strm.avail_in == 0);     /* all input will be used */

        /* done when last data in file processed */
    } while (flush != Z_FINISH);
    assert(ret == Z_STREAM_END);        /* stream will be complete */

    /* clean up and return */
    (void) deflateEnd(&strm);
    return Z_OK;
}

void temp_file_name(char *tmpnam)
{
    char tmppath[_MAX_PATH];
    tmppath[0] = tmpnam[0] = 0;
    GetTempPath(_MAX_PATH, tmppath);
    GetTempFileName(tmppath, "od_", 0, tmpnam);
}

int error(char *src, RETCODE rv, SQLSMALLINT htype, SQLHANDLE h)
{
    SQLSMALLINT i = 1;
    SQLCHAR     sql_state[6], msg[SQL_MAX_MESSAGE_LENGTH], buffer[24 * 1024], *enc;
    SQLINTEGER  error_id = 0;
    SQLSMALLINT msg_len = 0;
    int         length;

    if (IS_SQL_SUCCESS(rv))
        return 0;

    printf("ERROR=\"source=%s,code=%d", src, rv);

    if (h)
    {
        for (i = 1;
             SQLGetDiagRec(htype, h, i, sql_state, &error_id, msg, sizeof(msg), &msg_len) != SQL_NO_DATA;
             i++)
        {
            length = sprintf((char *) buffer, "\nSQL Error State: %s, Native Error Code: %lX, ODBC Error: %s",
                             (LPSTR) sql_state, error_id, (LPSTR) msg);
            enc = (SQLCHAR *) url_encode((char *) buffer, length, 0, NULL);
            fputs((char *) enc, stdout);
            free(enc);
        }
    }
    else
    {
        fputs(",NULL handle error", stdout);
    }

    fputs("\";", stdout);
    fflush(stdout);

    return rv;
}

void cleanup(SQLHENV henv, SQLHDBC dbh, SQLHSTMT sth)
{
    if (sth)
        SQLFreeHandle(SQL_HANDLE_STMT, sth);

    if (dbh)
    {
        SQLDisconnect(dbh);
        SQLFreeHandle(SQL_HANDLE_DBC, dbh);
    }

    if (henv)
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
}
