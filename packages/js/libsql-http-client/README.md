# libSQL HTTP driver for TypeScript and JavaScript

## Getting Started

To get started, you need `sqld` running somewhere. Then:

```typescript
import { connect } from "@libsql/client"

const config = {
  url: "http://localhost:8080"
};
const db = connect(config);
const rs = await db.execute("SELECT * FROM users");
console.log(rs);
```

You can't use this package in file/memory mode (`file:example.db` or `file::memory:`).
If you need this capability, please use `libsql-client` package instead.

We provide this http-only client to make it easier to integrate into node-less
environments where `fs` API is not available (such as CloudFlare Workers or in Browsers).

## Features

* SQLite JavaScript API
* SQL over HTTP with `fetch()`

## Roadmap

* Read replica mode
* Cloudflare D1 API compatibility?
