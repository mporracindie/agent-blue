import { WarehouseAdapter } from "../../core/interfaces.js";
import { QueryResult } from "../../core/types.js";

export class BigQueryWarehouseAdapter implements WarehouseAdapter {
  async query(_sql: string): Promise<QueryResult> {
    throw new Error("BigQuery adapter is not implemented yet.");
  }
}
