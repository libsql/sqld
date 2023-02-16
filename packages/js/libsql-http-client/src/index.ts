import { HttpDriver } from "./driver/http-driver";
import { Config, Connection } from "./shared-types";

export function connect(config: Config): Connection {
    const rawUrl = config.url;
    const url = new URL(rawUrl);
    if (url.protocol == "http:" || url.protocol == "https:") {
        return new Connection(new HttpDriver(url));
    } else {
        throw new Error(
            "libsql-http-client package supports only http connections. For memory of file storage, please use libsql-client package."
        );
    }
}

export * from "./shared-types";
export * from "./driver/driver";
export * from "./driver/http-driver";
