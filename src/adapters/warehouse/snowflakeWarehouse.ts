import snowflake from "snowflake-sdk";
import { WarehouseAdapter } from "../../core/interfaces.js";
import { QueryResult } from "../../core/types.js";

export interface SnowflakeConfig {
  account: string;
  username: string;
  password: string;
  warehouse: string;
  database: string;
  schema: string;
  role?: string;
}

export class SnowflakeWarehouseAdapter implements WarehouseAdapter {
  private readonly connection: snowflake.Connection;
  private connected = false;

  constructor(config: SnowflakeConfig) {
    this.connection = snowflake.createConnection({
      account: config.account,
      username: config.username,
      password: config.password,
      warehouse: config.warehouse,
      database: config.database,
      schema: config.schema,
      role: config.role
    });
  }

  private async ensureConnected(): Promise<void> {
    if (this.connected) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      this.connection.connect((err) => {
        if (err) {
          reject(err);
          return;
        }
        this.connected = true;
        resolve();
      });
    });
  }

  async query(sql: string, opts?: { timeoutMs?: number }): Promise<QueryResult> {
    await this.ensureConnected();

    const rows = await new Promise<Record<string, unknown>[]>((resolve, reject) => {
      this.connection.execute({
        sqlText: sql,
        complete: (err, _stmt, data) => {
          if (err) {
            reject(err);
            return;
          }
          resolve((data ?? []) as Record<string, unknown>[]);
        }
      });

      if (opts?.timeoutMs && opts.timeoutMs > 0) {
        setTimeout(() => reject(new Error(`Snowflake query timed out after ${opts.timeoutMs}ms`)), opts.timeoutMs);
      }
    });

    const columns = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
    return {
      columns,
      rows,
      rowCount: rows.length
    };
  }
}
