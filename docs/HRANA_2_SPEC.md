# The Hrana protocol specification (version 2)

Hrana (from Czech "hrana", which means "edge") is a protocol for connecting to a
SQLite database over a WebSocket. It is designed to be used from edge functions,
where low latency and small overhead is important.

In this specification, version 2 of the protocol is described as a set of
extensions to version 1.

## Version negotiation

The Hrana protocol version 2 uses a WebSocket subprotocol `hrana2`. The
WebSocket subprotocol negotiation allows the client and server to use version 2
of the protocol if both peers support it, but fall back to version 1 if the
client or the server don't support version 2.

## Messages

### Hello

The `hello` message has the same format as in version 1. The client must send it
as the first message, but in version 2, the client can also send it again
anytime during the lifetime of the connection to reauthenticate, by providing a
new JWT.

This feature is needed because in long-living connections, the JWT used to
authenticate the client may expire and the server may terminate the connection.
Using this feature, the client can provide a fresh JWT.

## Requests

Version 2 introduces two new requests:

```typescript
type Request =
    | ...
    | DescribeReq
    | SequenceReq

type Response =
    | ...
    | DescribeResp
    | SequenceResp
```

### Describe a statement

```typescript
type DescribeReq = {
    "type": "describe",
    "stream_id": int32,
    "sql": string,
}

type DescribeResp = {
    "type": "describe",
    "params": Array<DescribeParam>,
    "columns": Array<DescribeColumn>,
    "is_explain": boolean,
    "readonly": boolean,
}
```

The `describe` request is used to parse and analyze a SQL statement. `stream_id`
specifies the stream on which the statement is parsed and `sql` specifies the
SQL text.

In the response, `is_explain` is true if the statement was an `EXPLAIN`
statement, and `readonly` is true if the statement does not modify the database.

```typescript
type DescribeParam = {
    "name": string | null,
}
```

Information about parameters of the statement is returned in `params`. SQLite
indexes parameters from 1, so the first object in the `params` array describes
parameter 1.

For each parameter, the `name` field specifies the name of the parameter. For
parameters of the form `?NNN`, `:AAA`, `@AAA` and `$AAA`, the name includes the
initial `?`, `:`, `@` or `$` character. Parameters of the form `?` are nameless,
their `name` is `null`.

It is also possible that some parameters are not referenced in the statement, in
which case the `name` is also `null`.

```typescript
type DescribeColumn = {
    "name": string,
    "decltype": string | null,
}
```

Information about columns of the statement is returned in `columns`.

For each column, `name` specifies the name assigned by the SQL `AS` clause. For
columns without `AS` clause, the name is not specified.

For result columns that directly originate from tables in the database,
`decltype` specifies the declared type of the column. For other columns (such as
results of expressions), `decltype` is `null`.

### Execute a sequence of SQL statements

```typescript
type SequenceReq = {
    "type": "sequence",
    "stream_id": int32,
    "sql": string,
}

type SequenceResp = {
    "type": "sequence",
}
```

The `sequence` request executes a sequence of SQL statements separated by
semicolons. The results of these statements are ignored. If any statement fails,
the server will not execute the following statements and will return an error
response.
