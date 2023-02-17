import { HttpDriver, Connection, Config } from "@libsql/http-client";
import { SqliteDriver } from "./driver/SqliteDriver";

export function connect(config: Config): Connection {
    const rawUrl = config.url;
    const url = new URL(rawUrl);
    if (url.protocol == "http:" || url.protocol == "https:") {
        return new Connection(new HttpDriver(url));
    } else {
        return new Connection(new SqliteDriver(rawUrl));
    }
}
