/*
 * Copyright (C) Scott Weisman
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#if defined(WIN32)
#  include <windows.h>
#  include <fcntl.h>
#  include <io.h>
#  define SET_BINARY_MODE(file) _setmode(fileno(file), O_BINARY)
#  define DELETE_FILE(f) DeleteFile(f)
#  define strcasecmp _stricmp
#else
#  define SET_BINARY_MODE(file)
#  define DELETE_FILE(f) remove(f)
#  define MAX_PATH 260
#endif
#include <sql.h>
#include <sqlext.h>
#include "md5.h"
#include "zlib.h"

#define QUERY_BUFFER_SIZE 8192
#define Z_CHUNK (256 * 1024)
#define MIN_ZIP_LEN 128            /* payloads smaller than this are never compressed */
#define MIN_BLOB_BUFFER 32768      /* SQLGetData chunk size for long char/binary columns */
#define MAX_FIELD_BUFFER (1 << 20) /* upper bound on the SQLGetData buffer; longer data is chunked */

#define IS_SQL_SUCCESS(x) ((x) == SQL_SUCCESS || (x) == SQL_SUCCESS_WITH_INFO)

typedef struct
{
    SQLCHAR       col_name[128];
    SQLSMALLINT   col_name_len;
    SQLSMALLINT   data_type;   /* SQL type from SQLDescribeCol */
    SQLSMALLINT   c_type;      /* C type used with SQLGetData */
    SQLULEN       col_size;
    SQLSMALLINT   decimal_digits;
    SQLSMALLINT   nullable;
} s_col_data;

typedef struct
{
    char    id[64];
    char    md5[33];
    char    sql[QUERY_BUFFER_SIZE];
    int     zip;
    int     tables;
} s_request;

static const char field_sep = '\t', rec_sep = '\n';
static const char hex_digits[] = "0123456789ABCDEF";

/* set by error() when an ODBC diagnostic reports a connection-class (08xxx) SQLSTATE */
static int connection_lost = 0;

static int get_request(s_request *request);
static int emit_result(SQLHSTMT sth, SQLSMALLINT col_count, s_request *request);
static int sql_fetch(SQLHSTMT sth, SQLSMALLINT col_count, FILE *stream, char *md5, unsigned long *total_len);
static int temp_file_name(char *name);
static int error(const char *src, SQLRETURN rv, SQLSMALLINT htype, SQLHANDLE h);
static void error_text(const char *msg);
static unsigned long encode_out(FILE *stream, MD5Context *md5, const unsigned char *b, size_t len);
static int oddie_deflate(FILE *source, FILE *dest, int level);
static int connection_dead(SQLHDBC dbh);
static void cleanup(SQLHENV henv, SQLHDBC dbh, SQLHSTMT sth);

int main(int argc, char *argv[])
{
    SQLRETURN     rv;
    SQLSMALLINT   col_count;
    SQLLEN        row_count;
    SQLHENV       henv = SQL_NULL_HENV;
    SQLHDBC       dbh = SQL_NULL_HDBC;
    SQLHSTMT      sth = SQL_NULL_HSTMT;
    s_request     request;
    int           daemon = 0, failed, exit_code = 1;
    char          *query, *sql;

    memset(&request, 0, sizeof(request));

    SET_BINARY_MODE(stdout);

    if (argc < 2 || !argv[1])
    {
        printf("usage: %s dsn_string [sql]", argv[0]);
        return 0;
    }
    else if (!argv[2])
    {
        daemon = 1;
        query = request.sql;
#if defined(WIN32)
        SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
#endif
    }
    else
        query = argv[2];

    rv = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (error("SQLAllocHandle(env)", rv, SQL_HANDLE_ENV, henv) || !henv)
        goto CLEANUP;

    rv = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC2, SQL_IS_INTEGER);
    if (error("SQLSetEnvAttr", rv, SQL_HANDLE_ENV, henv))
        goto CLEANUP;

    rv = SQLAllocHandle(SQL_HANDLE_DBC, henv, &dbh);
    if (error("SQLAllocHandle(dbc)", rv, SQL_HANDLE_ENV, henv) || !dbh)
        goto CLEANUP;

    rv = SQLDriverConnect(dbh, NULL, (SQLCHAR *) argv[1], SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (error("SQLDriverConnect", rv, SQL_HANDLE_DBC, dbh))
        goto CLEANUP;

    exit_code = 0;

    if (daemon)
    {
        fputs("OK", stdout);
        fflush(stdout);
    }

    for (;;)
    {
        if (daemon ? !get_request(&request) : !query[0])
            break;

        sql = query;
        while (*sql && (unsigned char) *sql < 33)
            sql++;

        /* everything below is a response to this request, so the ID goes first */
        if (request.id[0])
        {
            fputs("ID=\"", stdout);
            encode_out(stdout, NULL, (unsigned char *) request.id, strlen(request.id));
            fputs("\",", stdout);
        }

        row_count = -1;
        col_count = 0;

        rv = SQLAllocHandle(SQL_HANDLE_STMT, dbh, &sth);
        if (error("SQLAllocHandle(stmt)", rv, SQL_HANDLE_DBC, dbh) || !sth)
            goto CLEANUP;

        if (request.tables)
        {
            /* SQLTables returns a standard result set: TABLE_CAT, TABLE_SCHEM,
               TABLE_NAME, TABLE_TYPE, REMARKS. NULL arguments mean "all". */
            rv = SQLTables(sth, NULL, 0, NULL, 0, NULL, 0, NULL, 0);
            failed = error("SQLTables", rv, SQL_HANDLE_STMT, sth);
        }
        else
        {
            rv = SQLExecDirect(sth, (SQLCHAR *) sql, SQL_NTS);
            failed = error("SQLExecDirect", rv, SQL_HANDLE_STMT, sth);
        }

        if (!failed)
        {
            rv = SQLNumResultCols(sth, &col_count);
            failed = error("SQLNumResultCols", rv, SQL_HANDLE_STMT, sth);
        }

        if (!failed && !request.tables && col_count < 1)
        {
            rv = SQLRowCount(sth, &row_count);
            failed = error("SQLRowCount", rv, SQL_HANDLE_STMT, sth);
        }

        if (!failed)
        {
            /* A result set (col_count > 0) always wins, so INSERT ... RETURNING and
               DELETE ... OUTPUT are returned as data. Otherwise a non-negative row
               count means INSERT/UPDATE/DELETE. DDL and friends report -1 and get an
               empty result. This also sidesteps drivers (MariaDB) that return a row
               count for SELECT in violation of the spec. */
            if (col_count > 0)
                emit_result(sth, col_count, &request);
            else if (row_count > -1)
                printf("ROWCOUNT=%ld;", (long) row_count);
            else
                fputs("RESULT=\"\";", stdout);

            fflush(stdout);
        }

        SQLFreeHandle(SQL_HANDLE_STMT, sth);
        sth = SQL_NULL_HSTMT;

        if (!daemon)
            break;

        /* Statement-level errors (bad SQL, constraint violations, ...) leave the
           connection usable, so keep serving. Only give up if the connection is gone. */
        if (failed && (connection_lost || connection_dead(dbh)))
        {
            exit_code = 1;
            break;
        }
    }

    CLEANUP:
    cleanup(henv, dbh, sth);

    return exit_code;
}

/*
 * Fetch the result set on sth to a temp file, then write the MD5/RESULT/ZIP
 * portion of the response. Returns 1 on success. On failure an ERROR response
 * has already been written.
 */
static int emit_result(SQLHSTMT sth, SQLSMALLINT col_count, s_request *request)
{
    FILE          *stream, *zstream;
    char          filename[MAX_PATH], zfilename[MAX_PATH], md5[33];
    unsigned char buffer[4096];
    unsigned long result_len = 0;
    size_t        n;
    int           level, ok;

    if (!temp_file_name(filename))
    {
        error_text("cannot create temp file");
        return 0;
    }

    stream = fopen(filename, "wb");
    if (!stream)
    {
        error_text("cannot open temp file");
        DELETE_FILE(filename);
        return 0;
    }

    ok = sql_fetch(sth, col_count, stream, md5, &result_len);
    fclose(stream);

    if (!ok)
    {
        DELETE_FILE(filename);
        return 0;
    }

    printf("MD5=%s,", md5);

    if (request->md5[0] && strcasecmp(md5, request->md5) == 0)
    {
        fputs("RESULT=CACHED;", stdout);
        DELETE_FILE(filename);
        return 1;
    }

    level = request->zip;
    if (level < 0)
        level = 0;
    else if (level > 9)
        level = 9;
    if (result_len < MIN_ZIP_LEN)
        level = 0;

    zfilename[0] = 0;

    if (level)
    {
        ok = 0;
        if (temp_file_name(zfilename))
        {
            stream = fopen(filename, "rb");
            zstream = fopen(zfilename, "wb");
            if (stream && zstream)
                ok = (oddie_deflate(stream, zstream, level) == Z_OK);
            if (stream)
                fclose(stream);
            if (zstream)
                fclose(zstream);
        }

        if (ok)
            printf("ZIP=%d,", level);
        else
        {
            /* compression failed; fall back to the raw result */
            if (zfilename[0])
                DELETE_FILE(zfilename);
            zfilename[0] = 0;
            level = 0;
        }
    }

    stream = fopen(level ? zfilename : filename, "rb");
    if (!stream)
    {
        error_text("cannot read temp file");
        ok = 0;
    }
    else
    {
        fputs("RESULT=\"", stdout);
        while ((n = fread(buffer, 1, sizeof(buffer), stream)) > 0)
            encode_out(stdout, NULL, buffer, n);
        fputs("\";", stdout);
        fclose(stream);
        ok = 1;
    }

    if (zfilename[0])
        DELETE_FILE(zfilename);
    DELETE_FILE(filename);

    return ok;
}

/* true if diagnostic record on sth reports SQLSTATE 01004 (data truncated) */
static int truncated(SQLHSTMT sth)
{
    SQLCHAR     state[6];
    SQLSMALLINT len, i;

    for (i = 1; ; i++)
    {
        SQLRETURN rv = SQLGetDiagField(SQL_HANDLE_STMT, sth, i, SQL_DIAG_SQLSTATE, state, sizeof(state), &len);
        if (!IS_SQL_SUCCESS(rv))
            return 0;
        if (memcmp(state, "01004", 5) == 0)
            return 1;
    }
}

/*
 * Write the header row and all data rows of sth to stream (tab/newline
 * separated, %XX encoded) and compute the MD5 of exactly the bytes written.
 * Returns 1 on success; on failure an ERROR response has been written.
 */
static int sql_fetch(SQLHSTMT sth, SQLSMALLINT col_count, FILE *stream, char *md5, unsigned long *total_len)
{
    SQLSMALLINT   i;
    SQLRETURN     rv;
    SQLLEN        ind, chunk;
    SQLULEN       buffer_size = 0, max_col_size = 0;
    unsigned char *buffer, md5_raw[16];
    MD5Context    md5_state;
    int           has_blob = 0, ok = 1;
    s_col_data    *col_data = (s_col_data *) calloc(col_count + 1, sizeof(s_col_data));

    *total_len = 0;

    if (!col_data)
    {
        error_text("out of memory");
        return 0;
    }

    /* columns are 1-based; slot 0 is unused */
    for (i = 1; i <= col_count; i++)
    {
        s_col_data *c = &col_data[i];

        rv = SQLDescribeCol(sth, i, c->col_name, sizeof(c->col_name), &c->col_name_len,
                            &c->data_type, &c->col_size, &c->decimal_digits, &c->nullable);
        if (!IS_SQL_SUCCESS(rv))
        {
            c->col_name[0] = 0;
            c->data_type = SQL_CHAR;
            c->col_size = 0;
        }
        c->col_name[sizeof(c->col_name) - 1] = 0;

        switch (c->data_type)
        {
            case SQL_LONGVARCHAR:
            case SQL_WLONGVARCHAR:
                c->c_type = SQL_C_CHAR;
                has_blob = 1;
                break;
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                c->c_type = SQL_C_BINARY;
                has_blob = 1;
                break;
            default:
                c->c_type = SQL_C_CHAR;
                break;
        }

        if (c->col_size > max_col_size)
            max_col_size = c->col_size;
    }

    /* Size the SQLGetData buffer from the widest column, doubled because some
       drivers under-report, with a floor for long columns and a cap so a huge
       declared size doesn't turn into a huge allocation. Anything that doesn't
       fit is retrieved in chunks. */
    if (max_col_size > MAX_FIELD_BUFFER / 2)
        max_col_size = MAX_FIELD_BUFFER / 2;
    buffer_size = max_col_size * 2 + 128;
    if (has_blob && buffer_size < MIN_BLOB_BUFFER)
        buffer_size = MIN_BLOB_BUFFER;
    if (buffer_size > MAX_FIELD_BUFFER)
        buffer_size = MAX_FIELD_BUFFER;

    buffer = (unsigned char *) malloc(buffer_size);
    if (!buffer)
    {
        free(col_data);
        error_text("out of memory");
        return 0;
    }

    MD5Init(&md5_state);

    /* header row; separators are hashed too so the MD5 reflects structure, not
       just the concatenated field bytes */
    for (i = 1; i <= col_count; i++)
    {
        *total_len += encode_out(stream, &md5_state, col_data[i].col_name, strlen((char *) col_data[i].col_name));
        if (i < col_count)
            *total_len += encode_out(stream, &md5_state, (const unsigned char *) &field_sep, 1);
    }
    *total_len += encode_out(stream, &md5_state, (const unsigned char *) &rec_sep, 1);

    for (;;)
    {
        rv = SQLFetch(sth);

        if (rv == SQL_NO_DATA)
            break;

        if (error("SQLFetch", rv, SQL_HANDLE_STMT, sth))
        {
            ok = 0;
            break;
        }

        for (i = 1; i <= col_count && ok; i++)
        {
            SQLSMALLINT c_type = col_data[i].c_type;

            for (;;)
            {
                rv = SQLGetData(sth, i, c_type, buffer, buffer_size, &ind);

                if (rv == SQL_NO_DATA)
                    break;

                if (error("SQLGetData", rv, SQL_HANDLE_STMT, sth))
                {
                    ok = 0;
                    break;
                }

                if (ind == SQL_NULL_DATA || ind == 0)
                    break;

                /* ind is the total length available, which exceeds the buffer when
                   the data was truncated. For SQL_C_CHAR the last buffer byte is the
                   NUL terminator and must not be copied. */
                chunk = (c_type == SQL_C_CHAR) ? (SQLLEN) buffer_size - 1 : (SQLLEN) buffer_size;
                if (ind != SQL_NO_TOTAL && ind < chunk)
                    chunk = ind;

                *total_len += encode_out(stream, &md5_state, buffer, (size_t) chunk);

                /* SQL_SUCCESS_WITH_INFO with 01004 means more data for this column */
                if (rv == SQL_SUCCESS_WITH_INFO && truncated(sth))
                    continue;

                break;
            }

            if (ok && i < col_count)
                *total_len += encode_out(stream, &md5_state, (const unsigned char *) &field_sep, 1);
        }

        if (!ok)
            break;

        *total_len += encode_out(stream, &md5_state, (const unsigned char *) &rec_sep, 1);
    }

    fflush(stream);
    free(col_data);
    free(buffer);
    MD5Final(md5_raw, &md5_state);

    for (i = 0; i < 16; i++)
    {
        md5[i * 2] = hex_digits[md5_raw[i] >> 4];
        md5[i * 2 + 1] = hex_digits[md5_raw[i] & 15];
    }
    md5[32] = 0;

    return ok;
}

#define tNONE    0
#define tID      1
#define tMD5     2
#define tZIP     3
#define tSQL     4
#define tTABLES  5
#define tCLOSE   6

/* copy the parsed value into a fixed request field; false if it doesn't fit */
static int set_field(char *dest, size_t dest_size, const char *src, size_t len)
{
    if (len >= dest_size)
        return 0;
    memcpy(dest, src, len);
    dest[len] = 0;
    return 1;
}

/*
 * Read one semicolon-terminated request from stdin into request.
 * Returns 1 when a request is ready, 0 on CLOSE or end of input.
 * Malformed requests get an ERROR response and are skipped.
 */
static int get_request(s_request *request)
{
    char   buffer[QUERY_BUFFER_SIZE + 256];
    size_t pos = 0;
    int    target = tNONE, c, hi, lo, seen = 0;
    const char *bad = NULL;

    request->zip = request->tables = 0;
    request->id[0] = request->md5[0] = request->sql[0] = buffer[0] = 0;

    while ((c = fgetc(stdin)) != EOF)
    {
        if (c == '=')
        {
            buffer[pos] = 0;
            seen = 1;

            switch (buffer[0])
            {
                case 'I': target = tID;     break;
                case 'M': target = tMD5;    break;
                case 'Z': target = tZIP;    break;
                case 'S': target = tSQL;    break;
                case 'T': target = tTABLES; break;
                case 'C': target = tCLOSE;  break;
                default:
                    target = tNONE;
                    if (!bad)
                        bad = "unknown field";
                    break;
            }

            buffer[pos = 0] = 0;
        }
        else if (c == ',' || c == ';')
        {
            buffer[pos] = 0;

            if (!bad)
            {
                switch (target)
                {
                    case tID:
                        if (!set_field(request->id, sizeof(request->id), buffer, pos))
                            bad = "ID field too long";
                        break;
                    case tMD5:
                        if (!set_field(request->md5, sizeof(request->md5), buffer, pos))
                            bad = "MD5 field too long";
                        break;
                    case tSQL:
                        if (!set_field(request->sql, sizeof(request->sql), buffer, pos))
                            bad = "SQL query exceeds 8191 byte limit";
                        break;
                    case tZIP:
                        request->zip = atoi(buffer);
                        break;
                    case tTABLES:
                        request->tables = atoi(buffer);
                        break;
                    case tCLOSE:
                        return 0;
                    default:
                        if (seen || pos)
                            bad = "malformed request";
                        break;
                }
            }

            buffer[pos = 0] = 0;
            target = tNONE;

            if (c == ';')
            {
                if (!seen)
                    continue;               /* stray terminator between requests */

                if (bad)
                {
                    error_text(bad);
                    /* start over on the next request */
                    bad = NULL;
                    seen = 0;
                    request->zip = request->tables = 0;
                    request->id[0] = request->md5[0] = request->sql[0] = 0;
                    continue;
                }

                if (!request->tables && !request->sql[0])
                {
                    error_text("request has no SQL");
                    seen = 0;
                    request->zip = request->tables = 0;
                    request->id[0] = request->md5[0] = 0;
                    continue;
                }

                return 1;
            }
        }
        else if (c == '"')
        {
            while ((c = fgetc(stdin)) != EOF && c != '"')
            {
                if (c == '%')
                {
                    hi = fgetc(stdin);
                    lo = fgetc(stdin);
                    if (hi == EOF || lo == EOF)
                        return 0;
                    if (!isxdigit(hi) || !isxdigit(lo))
                    {
                        if (!bad)
                            bad = "bad %XX escape";
                        continue;
                    }
                    hi = isdigit(hi) ? hi - '0' : (tolower(hi) - 'a' + 10);
                    lo = isdigit(lo) ? lo - '0' : (tolower(lo) - 'a' + 10);
                    c = (hi << 4) | lo;
                }

                if (pos < sizeof(buffer) - 1)
                    buffer[pos++] = (char) c;
                else if (!bad)
                    bad = "request too long";
            }

            buffer[pos] = 0;

            if (c == EOF)
                return 0;
        }
        else if (isalnum(c))
        {
            if (pos < sizeof(buffer) - 1)
                buffer[pos++] = (char) c;
            else if (!bad)
                bad = "request too long";
        }
        /* anything else outside quotes (whitespace, newlines) is ignored */
    }

    return 0;
}

/*
 * Write len bytes of b to stream with protocol-reserved bytes %XX encoded.
 * If md5 is given the encoded bytes are hashed as well. Returns bytes written.
 */
static unsigned long encode_out(FILE *stream, MD5Context *md5, const unsigned char *b, size_t len)
{
    unsigned char out[4096];
    size_t        i, n = 0;
    unsigned long total = 0;

    for (i = 0; i < len; i++)
    {
        unsigned char c = b[i];

        if (n + 3 > sizeof(out))
        {
            fwrite(out, 1, n, stream);
            if (md5)
                MD5Update(md5, out, (unsigned) n);
            total += n;
            n = 0;
        }

        if (c < 32 || c == '"' || c == '%' || c == ';' || c == ',' || c == '=')
        {
            out[n++] = '%';
            out[n++] = hex_digits[c >> 4];
            out[n++] = hex_digits[c & 15];
        }
        else
            out[n++] = c;
    }

    if (n)
    {
        fwrite(out, 1, n, stream);
        if (md5)
            MD5Update(md5, out, (unsigned) n);
        total += n;
    }

    return total;
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
static int oddie_deflate(FILE *source, FILE *dest, int level)
{
    int ret, flush;
    unsigned have;
    z_stream strm;
    unsigned char *in = (unsigned char *) malloc(Z_CHUNK);
    unsigned char *out = (unsigned char *) malloc(Z_CHUNK);

    if (!in || !out)
    {
        free(in);
        free(out);
        return Z_MEM_ERROR;
    }

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, level);
    if (ret != Z_OK)
    {
        free(in);
        free(out);
        return ret;
    }

    /* compress until end of file */
    do {
        strm.avail_in = fread(in, 1, Z_CHUNK, source);
        if (ferror(source)) {
            ret = Z_ERRNO;
            goto DONE;
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
                ret = Z_ERRNO;
                goto DONE;
            }
        } while (strm.avail_out == 0);
        assert(strm.avail_in == 0);     /* all input will be used */

        /* done when last data in file processed */
    } while (flush != Z_FINISH);
    assert(ret == Z_STREAM_END);        /* stream will be complete */
    ret = Z_OK;

    DONE:
    (void) deflateEnd(&strm);
    free(in);
    free(out);
    return ret;
}

/* generate a unique temp file name into name (MAX_PATH bytes); true on success */
static int temp_file_name(char *name)
{
    name[0] = 0;
#if defined(WIN32)
    {
        char tmppath[MAX_PATH];
        DWORD n = GetTempPath(sizeof(tmppath), tmppath);
        if (n == 0 || n >= sizeof(tmppath))
            return 0;
        return GetTempFileName(tmppath, "od_", 0, name) != 0;
    }
#else
    return tmpnam(name) != NULL;
#endif
}

/* write a complete ERROR response for a non-ODBC failure */
static void error_text(const char *msg)
{
    fputs("ERROR=\"", stdout);
    encode_out(stdout, NULL, (const unsigned char *) msg, strlen(msg));
    fputs("\";", stdout);
    fflush(stdout);
}

/*
 * If rv is a failure, write an ERROR response carrying the ODBC diagnostic
 * chain for h and return non-zero. SQL_SUCCESS_WITH_INFO is not a failure.
 */
static int error(const char *src, SQLRETURN rv, SQLSMALLINT htype, SQLHANDLE h)
{
    SQLSMALLINT i;
    SQLCHAR     sql_state[6], msg[SQL_MAX_MESSAGE_LENGTH], buffer[SQL_MAX_MESSAGE_LENGTH + 128];
    SQLINTEGER  native = 0;
    SQLSMALLINT msg_len = 0;
    int         length;

    if (IS_SQL_SUCCESS(rv))
        return 0;

    printf("ERROR=\"source=%s,code=%d", src, (int) rv);

    if (h && rv != SQL_INVALID_HANDLE)
    {
        for (i = 1; ; i++)
        {
            SQLRETURN drv = SQLGetDiagRec(htype, h, i, sql_state, &native, msg, sizeof(msg), &msg_len);
            if (!IS_SQL_SUCCESS(drv))
                break;

            sql_state[5] = 0;
            msg[sizeof(msg) - 1] = 0;

            if (sql_state[0] == '0' && sql_state[1] == '8')
                connection_lost = 1;

            length = sprintf((char *) buffer, "\nSQL Error State: %s, Native Error Code: %ld, ODBC Error: %s",
                             (char *) sql_state, (long) native, (char *) msg);
            encode_out(stdout, NULL, buffer, (size_t) length);
        }
    }
    else
    {
        fputs(",NULL handle error", stdout);
    }

    fputs("\";", stdout);
    fflush(stdout);

    return 1;
}

/* true if the driver reports the connection as dead */
static int connection_dead(SQLHDBC dbh)
{
    SQLUINTEGER dead = SQL_CD_FALSE;
    SQLRETURN rv = SQLGetConnectAttr(dbh, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);

    return IS_SQL_SUCCESS(rv) && dead == SQL_CD_TRUE;
}

static void cleanup(SQLHENV henv, SQLHDBC dbh, SQLHSTMT sth)
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
