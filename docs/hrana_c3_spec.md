# The Hrana-C3 protocol specification

The Hrana-C3 protocol is an extension to the Hrana protocol that can be used to
implement a large subset of the [sqlite3 C API][sqlite3-c-api] over the network.

[sqlite3-c-api]: https://www.sqlite.org/c3ref/intro.html

## Motivation

The Hrana protocol aims to be a useful general-purpose protocol for connecting
to a SQL database from an edge function. However, the protocol cannot be used to
implement the sqlite3 C API, which requires low level access to the database.

To avoid introducing unnecessary complexity in the Hrana protocol, we formulate
Hrana-C3 as an optional extension to the Hrana protocol. It is expected that the
only server implementation will be `sqld` and the only client implementation
will be `sqlc` (the sqlite3-C-API-compatible client library for `sqld`).

## Usage

The only goal of this extension is compatiblity with sqlite3 C API. For other
uses, this protocol unnecessary slow and complicated and the original protocol
should be used instead.

## Overview

This protocol uses the WebSocket subprotocol `hrana1-c3`. Client and server will
negotiate the subprotocol using the `Sec-WebSocket-Protocol` header in the
WebSocket handshake.

A Hrana stream corresponds to the database connection object `sqlite3` in the C
API. This extension also introduces _statements_, which are associated with
streams and correspond to `sqlite3_stmt` in the C API.

## Extensions to existing messages

The `Error` type in the `ResponseErrorMsg` message is extended as follows:

```
type Error = {
    "message": string,
    "code_c3"?: int32 | null,
    "offset_c3"?: int32 | null,
}
```

For errors that correspond to errors in the C API, the `Error` looks as follows:
- `Error.message` is the result from `sqlite3_errmsg()`,
- `Error.code_c3` is the extended error code from `sqlite3_extended_errcode()`,
- `Error.offset_c3` is the byte offset in the input SQL from
`sqlite3_error_offset()`.

## Additional requests

This extension introduces new request/response types:

```
type Request =
    | ...
    | PrepareReqC3

type ResponseC3 =
    | ...
    | PrepareRespC3
```

### Prepare statement

```
type PrepareStmtReqC3 = {
    "type": "prepare_stmt_c3",
    "stream_id": int32,
    "stmt_id": int32,
    "sql": string,
    "save_sql": boolean,
    "flags": uint32,
}

type PrepareStmtRespC3 = {
    "type": "prepare_stmt_c3",
    "sql_len": uint32,
    "params": Array<ParamC3>,
    "isexplain": int32,
    "readonly": boolean,
}

type ParamC3 = {
    "name": string | null,
}
```

The `prepare_c3` request corresponds to the variants of the `sqlite3_prepare()`
function. `stream_id` specifies the stream that the statement belongs to and
`stmt_id` is an arbitrary 32-bit identifier of the created statement. Statement
identifiers need to be unique for the whole connection.

Other fields in the request are:
- `sql` is the SQL string (`zSql` in the C API),
- `save_sql` is true if the `_v2` or `_v3` versions of the prepare function
should be used (this corresponds to an internal sqlite flag
`SQLITE_PREPARE_SAVESQL`)
- `flags` are the `SQLITE_PREPARE_` flags (`prepFlags` in the C API).

Fields in the response are:
- `sql_len` returns the number of bytes in UTF-8
encoding of `sql` that correspond to the SQL statement that was parsed (this
corresponds to `pzTail` in the C API).
- `params` returns information about the number of parameters in the statement
and their names (corresponds to `sqlite3_bind_parameter_{count,index,name}()` in
the C API)
- `isexplain` is 1 if the statement is an EXPLAIN statement, 2 if it is EXPLAIN
QUERY PLAN, and 0 otherwise (corresponds to `sqlite3_stmt_isexplain()` in the
C API)
- `readonly` is true if the statement is read-only (corresponds to
`sqlite3_stmt_readonly()`).

### Close statement

```
type CloseStmtReqC3 = {
    "type": "close_stmt_c3",
    "stmt_id": int32,
}

type CloseStmtRespC3 = {
    "type": "close_stmt_c3",
}
```

The `close_stmt_c3` request closes a statement. It corresponds to
`sqlite3_finalize()` in the C API.

### Bind values

```
type StmtBindReqC3 = {
    "type": "stmt_bind_c3",
    "stmt_id": int32,
    "clear": boolean,
    "bindings": Array<BindingC3>,
}

type BindingC3 = {
    "param": int32,
    "value": BindingValueC3,
}

type BindingValueC3 =
    | Value
    | { "type": "zeroblob", "len": int32 }

type StmtBindRespC3 = {
    "type": "stmt_bind_c3",
}
```

The `stmt_bind_c3` request sets values for an arbitrary amount of parameters of
the given statement. Parameters are identified by their 1-based index. This
corresponds to the `sqlite3_bind_*()` family of functions in the C API.

If the `clear` field is set to true in the request, all previous bindings are
set to NULL before the new bindings are applied. This corresponds to the
`sqlite3_clear_bindings()` function in the C API.

### Evaluate statement

```
type StmtStepReqC3 = {
    "type": "stmt_step_c3",
    "stmt_id": int32,
}

type StmtStepRespC3 = {
    "type": "stmt_step_c3",
    "row": Array<Value> | null,
}
```

The `stmt_step_c3` request evaluates the statement and returns the next row that
it produced. If there are no more rows, the `row` is null. This corresponds to
`sqlite3_step()` in the C API.

### Reset statement

```
type StmtResetReqC3 = {
    "type": "stmt_reset_c3",
    "stmt_id": int32,
}

type StmtResetRespC3 = {
    "type": "stmt_reset_c3",
}
```

The `stmt_reset_c3` request resets a prepared statement to its initial state,
but keeps the bound values unchanged. This corresponds to `sqlite3_reset()`.

### Describe columns

```
type StmtDescribeReqC3 = {
    "type": "stmt_describe_c3",
    "stmt_id": int32,
}

type StmtDescribeRespC3 = {
    "type": "stmt_describe_c3",
    "columns": Array<ColumnC3>,
}

type ColumnC3 = {
    "decltype": string | null,
    "database_name": string | null,
    "table_name": string | null,
    "origin_name": string | null,
}
```

The `stmt_describe_c3` request returns information about columns using the
current state of the statement. The fields in the `columns` array are:

- `decltype` is the declared type of the table column
(`sqlite3_column_decltype()` in C API)
- `database_name`, `table_name` and `origin_name` locate the table column
(`sqlite3_column_{database,table,origin}_name()` in C API)
