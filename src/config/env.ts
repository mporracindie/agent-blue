import { config as dotenvConfig } from "dotenv";

dotenvConfig();

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

export const env = {
  appDataDir: process.env.APP_DATA_DIR ?? "data",
  llmBaseUrl: process.env.LLM_BASE_URL ?? "https://openrouter.ai/api/v1",
  llmApiKey: process.env.LLM_API_KEY ?? "",
  llmModel: process.env.LLM_MODEL ?? "openai/gpt-4o-mini",
  snowflakeAccount: process.env.SNOWFLAKE_ACCOUNT ?? "",
  snowflakeUsername: process.env.SNOWFLAKE_USERNAME ?? "",
  snowflakePassword: process.env.SNOWFLAKE_PASSWORD ?? "",
  snowflakeWarehouse: process.env.SNOWFLAKE_WAREHOUSE ?? "",
  snowflakeDatabase: process.env.SNOWFLAKE_DATABASE ?? "",
  snowflakeSchema: process.env.SNOWFLAKE_SCHEMA ?? "",
  snowflakeRole: process.env.SNOWFLAKE_ROLE ?? "",
  require(name: string): string {
    return required(name);
  }
};
