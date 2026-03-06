import path from "node:path";
import { env } from "./config/env.js";
import { ChartJsTool } from "./adapters/chart/chartJsTool.js";
import { OpenAiCompatibleProvider } from "./adapters/llm/openAiCompatibleProvider.js";
import { SqliteConversationStore } from "./adapters/store/sqliteConversationStore.js";
import { SnowflakeConfig, SnowflakeWarehouseAdapter } from "./adapters/warehouse/snowflakeWarehouse.js";
import { GitDbtRepositoryService } from "./adapters/dbt/dbtRepoService.js";
import { SqlGuard } from "./core/sqlGuard.js";
import { AnalyticsAgentRuntime } from "./core/agentRuntime.js";
import type { TenantWarehouseConfig, WarehouseAdapter } from "./core/interfaces.js";

export function buildStore(): SqliteConversationStore {
  const dbPath = path.join(env.appDataDir, "agent.db");
  const store = new SqliteConversationStore(dbPath);
  store.init();
  return store;
}

export function buildRuntime(store: SqliteConversationStore): AnalyticsAgentRuntime {
  const llm = buildLlmProvider();
  let defaultWarehouse: WarehouseAdapter | null = null;
  const warehouseResolver = (tenantId: string) => {
    const config = store.getTenantWarehouseConfig(tenantId);
    if (config) {
      return buildWarehouseFromTenantConfig(config);
    }
    // Lazily build fallback warehouse to avoid startup failure when global env
    // is intentionally unset and tenant-specific warehouse config is used.
    if (!defaultWarehouse) {
      defaultWarehouse = buildSnowflakeWarehouse();
    }
    return defaultWarehouse;
  };
  const chartTool = new ChartJsTool();
  const dbtRepo = new GitDbtRepositoryService(store);
  const sqlGuard = new SqlGuard({
    enforceReadOnly: true,
    defaultLimit: 200,
    maxLimit: 2000
  });

  return new AnalyticsAgentRuntime(llm, warehouseResolver, chartTool, dbtRepo, store, sqlGuard);
}

export function buildLlmProvider(): OpenAiCompatibleProvider {
  return new OpenAiCompatibleProvider(env.llmBaseUrl, env.llmApiKey, {
    "HTTP-Referer": "https://agent-blue.local",
    "X-Title": "agent-blue"
  });
}

export function buildSnowflakeConfig(): SnowflakeConfig {
  if (env.snowflakeAuthType === "keypair") {
    if (!env.snowflakePrivateKeyPath) {
      throw new Error("SNOWFLAKE_PRIVATE_KEY_PATH is required when SNOWFLAKE_AUTH_TYPE=keypair.");
    }
    return {
      account: env.snowflakeAccount,
      username: env.snowflakeUsername,
      warehouse: env.snowflakeWarehouse,
      database: env.snowflakeDatabase,
      schema: env.snowflakeSchema,
      role: env.snowflakeRole || undefined,
      logLevel: env.snowflakeSdkLogLevel as SnowflakeConfig["logLevel"],
      auth: {
        type: "keypair",
        privateKeyPath: env.snowflakePrivateKeyPath,
        privateKeyPassphrase: env.snowflakePrivateKeyPassphrase || undefined
      }
    };
  }

  return {
    account: env.snowflakeAccount,
    username: env.snowflakeUsername,
    warehouse: env.snowflakeWarehouse,
    database: env.snowflakeDatabase,
    schema: env.snowflakeSchema,
    role: env.snowflakeRole || undefined,
    logLevel: env.snowflakeSdkLogLevel as SnowflakeConfig["logLevel"],
    auth: {
      type: "password",
      password: env.snowflakePassword
    }
  };
}

export function buildSnowflakeWarehouse(): SnowflakeWarehouseAdapter {
  return new SnowflakeWarehouseAdapter(buildSnowflakeConfig());
}

export function buildWarehouseFromTenantConfig(config: TenantWarehouseConfig): WarehouseAdapter {
  if (config.provider === "bigquery") {
    throw new Error("BigQuery warehouse adapter is not implemented yet.");
  }
  const sf = config.snowflake;
  if (!sf) {
    throw new Error("Snowflake config missing for tenant.");
  }
  let auth: SnowflakeConfig["auth"];
  if (sf.authType === "keypair") {
    if (!sf.privateKeyPath) {
      throw new Error("privateKeyPath required for keypair auth.");
    }
    auth = {
      type: "keypair",
      privateKeyPath: sf.privateKeyPath,
      privateKeyPassphrase: undefined
    };
  } else {
    const passwordEnvVar = sf.passwordEnvVar ?? "SNOWFLAKE_PASSWORD";
    const password = process.env[passwordEnvVar] ?? "";
    if (!password) {
      throw new Error(
        `Password not found. Set env var ${passwordEnvVar} or configure passwordEnvVar in tenant warehouse config.`
      );
    }
    auth = { type: "password", password };
  }
  const snowflakeConfig: SnowflakeConfig = {
    account: sf.account,
    username: sf.username,
    warehouse: sf.warehouse,
    database: sf.database,
    schema: sf.schema,
    role: sf.role,
    logLevel: (env.snowflakeSdkLogLevel ?? "OFF") as SnowflakeConfig["logLevel"],
    auth
  };
  return new SnowflakeWarehouseAdapter(snowflakeConfig);
}
