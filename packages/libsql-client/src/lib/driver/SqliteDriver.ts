import DatabaseConstructor, {Database} from "better-sqlite3";
import { ResultSet } from "../libsql-js";
import { Driver } from "./Driver";

export class SqliteDriver implements Driver {
    db: Database;
    constructor(url: string) {
        this.db = new DatabaseConstructor(url)
    }
    async transaction(sqls: string[]): Promise<ResultSet[]> {
        const result = [];
        for (let sql of sqls) {
            const rs = await this.execute(sql);
            result.push(rs);
        }
        return result;
    }
    async execute(sql: string): Promise<ResultSet> {
        return await new Promise(resolve => {
            const stmt = this.db.prepare(sql);
            let columns: string[] = [];
            let rows: any[] = [];
            try {
                columns = stmt.columns().map(c => c.name);
                rows = stmt.all().map(row => {
                    return columns.map(column => row[column]);
                });
	    } catch (error) {
		// The better-sqlite3 API has a interface issue where SQL
		// statements that don't return results throw an exception,
		// but there is no way to check if that's the case. Therefore,
		// let's use this ugly hack where we catch the exception and
		// keep going.
                if (error != "TypeError: The columns() method is only for statements that return data") {
                    throw error;
                }
            }
            // FIXME: error handling
            const rs = {
                columns,
                rows,
                success: true,
                meta: {
                    duration: 0,
                },
            };
            resolve(rs);
        });
    }
}
