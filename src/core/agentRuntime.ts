import { z } from "zod";
import {
  ChartBuildRequest,
  ChartTool,
  ConversationStore,
  DbtRepositoryService,
  LlmMessage,
  LlmProvider,
  WarehouseAdapter
} from "./interfaces.js";
import { AgentArtifact, AgentContext, AgentResponse, QueryResult } from "./types.js";
import { SqlGuard } from "./sqlGuard.js";

const metadataLookupSchema = z.object({
  kind: z.enum(["schemas", "tables", "columns"]),
  database: z.string().optional(),
  schema: z.string().optional(),
  table: z.string().optional(),
  search: z.string().optional()
});

const chartRequestSchema = z.object({
  type: z.enum(["bar", "line", "pie", "doughnut"]).optional(),
  title: z.string().optional(),
  xKey: z.string().optional(),
  yKey: z.string().optional(),
  seriesKey: z.string().optional(),
  maxPoints: z.number().int().positive().max(500).optional()
});

const plannerSchema = z.object({
  action: z.enum(["answer", "query_snowflake", "inspect_dbt_model", "lookup_snowflake_metadata", "build_chart"]),
  answer: z.string().optional(),
  sql: z.string().optional(),
  modelName: z.string().optional(),
  metadataLookup: metadataLookupSchema.optional(),
  chartRequest: chartRequestSchema.optional(),
  reasoning: z.string().optional()
});

function asJsonBlock(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function quoteSqlIdent(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

function sqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function buildMetadataLookupSql(
  lookup: z.infer<typeof metadataLookupSchema>,
  defaultDatabase: string,
  defaultSchema: string,
  maxRows: number
): string | null {
  const database = (lookup.database?.trim() || defaultDatabase).toUpperCase();
  const schema = (lookup.schema?.trim() || defaultSchema).toUpperCase();
  const table = (lookup.table?.trim() || "").toUpperCase();
  const search = lookup.search?.trim();
  if (!database) {
    return null;
  }

  const informationSchema = `${quoteSqlIdent(database)}.INFORMATION_SCHEMA`;
  if (lookup.kind === "schemas") {
    const where = search ? `WHERE SCHEMA_NAME ILIKE ${sqlLiteral(`%${search}%`)}` : "";
    return `SELECT SCHEMA_NAME FROM ${informationSchema}.SCHEMATA ${where} ORDER BY SCHEMA_NAME LIMIT ${maxRows}`;
  }
  if (lookup.kind === "tables") {
    const where: string[] = [];
    if (schema) {
      where.push(`TABLE_SCHEMA = ${sqlLiteral(schema)}`);
    }
    if (search) {
      where.push(`TABLE_NAME ILIKE ${sqlLiteral(`%${search}%`)}`);
    }
    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    return `SELECT TABLE_SCHEMA, TABLE_NAME FROM ${informationSchema}.TABLES ${whereClause} ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT ${maxRows}`;
  }

  const where: string[] = [];
  if (schema) {
    where.push(`TABLE_SCHEMA = ${sqlLiteral(schema)}`);
  }
  if (table) {
    where.push(`TABLE_NAME = ${sqlLiteral(table)}`);
  }
  if (search) {
    where.push(`COLUMN_NAME ILIKE ${sqlLiteral(`%${search}%`)}`);
  }
  const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  return `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ${informationSchema}.COLUMNS ${whereClause} ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION LIMIT ${maxRows}`;
}

function inferSchemaHintFromModelPath(relativePath: string, fallbackSchema: string): string {
  const normalized = relativePath.replace(/\\/g, "/").toLowerCase();
  if (normalized.includes("/marts/")) {
    return "MARTS";
  }
  if (normalized.includes("/intermediate/") || normalized.includes("/int/")) {
    return "INT";
  }
  if (normalized.includes("/staging/") || normalized.includes("/stg/")) {
    return "STAGING";
  }
  if (normalized.includes("/core/")) {
    return "CORE";
  }
  return fallbackSchema || "PUBLIC";
}

export class AnalyticsAgentRuntime {
  constructor(
    private readonly llm: LlmProvider,
    private readonly warehouse: WarehouseAdapter,
    private readonly chartTool: ChartTool,
    private readonly dbtRepo: DbtRepositoryService,
    private readonly store: ConversationStore,
    private readonly sqlGuard: SqlGuard
  ) {}

  async respond(context: AgentContext, userText: string): Promise<AgentResponse> {
    const startedAt = Date.now();
    const timings: Record<string, number> = {};
    const maxPlannerSteps = 6;
    const plannerAttempts: Array<{ step: number; raw?: string; parseError?: string; plan?: Record<string, unknown> }> = [];
    const attemptedSql = new Set<string>();
    const toolCalls: Array<{
      tool: string;
      input: Record<string, unknown>;
      status: "ok" | "error";
      durationMs: number;
      outputSummary?: Record<string, unknown>;
      output?: unknown;
      error?: string;
    }> = [];
    const measure = async <T>(label: string, fn: () => Promise<T>): Promise<T> => {
      const stepStart = Date.now();
      const result = await fn();
      timings[label] = Date.now() - stepStart;
      return result;
    };
    const runTool = async <T>(
      tool: string,
      input: Record<string, unknown>,
      fn: () => Promise<T>,
      summarize?: (value: T) => Record<string, unknown>,
      fullOutput?: (value: T) => unknown
    ): Promise<T> => {
      const start = Date.now();
      try {
        const value = await fn();
        toolCalls.push({
          tool,
          input,
          status: "ok",
          durationMs: Date.now() - start,
          outputSummary: summarize ? summarize(value) : undefined,
          output: fullOutput ? fullOutput(value) : undefined
        });
        return value;
      } catch (error) {
        toolCalls.push({
          tool,
          input,
          status: "error",
          durationMs: Date.now() - start,
          error: (error as Error).message
        });
        throw error;
      }
    };

    this.store.createConversation(context);
    this.store.addMessage({
      tenantId: context.tenantId,
      conversationId: context.conversationId,
      role: "user",
      content: userText
    });

    const profile = this.store.getOrCreateProfile(context.tenantId, context.profileName);
    timings.profileMs = Date.now() - startedAt;
    const history = this.store.getMessages(context.conversationId, 12);
    const tenantRepo = this.store.getTenantRepo(context.tenantId);
    const snowflakeDatabase = process.env.SNOWFLAKE_DATABASE?.trim() ?? "";
    const snowflakeSchema = process.env.SNOWFLAKE_SCHEMA?.trim() ?? "";
    const llmModel = context.llmModel?.trim() || process.env.LLM_MODEL || "openai/gpt-4o-mini";
    const now = new Date();
    const currentDateIso = now.toISOString();
    const currentDate = currentDateIso.slice(0, 10);
    const hasWarehouseDefaults = snowflakeDatabase.length > 0 && snowflakeSchema.length > 0;
    const fqPrefix = hasWarehouseDefaults
      ? `${quoteSqlIdent(snowflakeDatabase)}.${quoteSqlIdent(snowflakeSchema)}`
      : "";
    const dbtModels = await measure("dbtModelsMs", async () => {
      try {
        return await runTool(
          "dbt.listModels",
          { tenantId: context.tenantId },
          async () => this.dbtRepo.listModels(context.tenantId),
          (models) => ({ modelCount: models.length })
        );
      } catch {
        return [];
      }
    });
    const schemaCandidates = Array.from(
      new Set(
        [snowflakeSchema.toUpperCase(), "INT", "MARTS", "STAGING", "CORE", "PUBLIC"].filter(
          (value) => value.length > 0
        )
      )
    );

    const historyMessages: LlmMessage[] = history
      .filter((m) => m.role !== "tool" && m.role !== "system")
      .map((m): LlmMessage => ({
        role: m.role === "user" ? "user" : "assistant",
        content: m.content
      }));

    const planningMessages = (): LlmMessage[] => [
      {
        role: "system",
        content: [
          profile.soulPrompt,
          "",
          "You are an analytics orchestrator. Decide if you can directly answer the user,",
          "need to query Snowflake, inspect warehouse metadata (schemas/tables/columns), inspect a dbt model file, or build a chart config.",
          `Current date/time (UTC): ${currentDateIso}`,
          `Current date (UTC): ${currentDate}`,
          "You can run multiple tool calls iteratively before providing the final answer.",
          "Prefer accuracy over speed. If table lineage is unclear, inspect a dbt model before querying.",
          "",
          "SQL generation requirements (strict):",
          "- Use fully-qualified Snowflake object names in every query: DATABASE.SCHEMA.OBJECT.",
          "- Never use unqualified table names like `fct_transactions`.",
          hasWarehouseDefaults
            ? `- Start with ${fqPrefix} as a default guess, but do not treat schema as fixed.`
            : "- SNOWFLAKE_DATABASE/SCHEMA defaults are unavailable, so infer carefully and avoid guessing.",
          `- Allowed/expected schema candidates to consider: ${schemaCandidates.join(", ")}.`,
          "- Use dbt model path hints and inspected dbt SQL to choose schema (for example, marts -> MARTS, intermediate/int -> INT).",
          "- Prefer relations that match synced dbt model names.",
          "- When uncertain about the correct relation, return action `inspect_dbt_model` first.",
          "- If table/schema/column names are uncertain, return action `lookup_snowflake_metadata` first.",
          "- If the user requests a visualization, prefer action `build_chart` after at least one successful query.",
          "- If a Snowflake query fails, inspect the tool error and return a corrected SQL query.",
          "- After relation-not-found errors, try alternative schemas and avoid assuming only one schema.",
          "- Do not repeat the exact same failing SQL.",
          "",
          "Return ONLY valid JSON with fields:",
          '{ "action": "answer|query_snowflake|inspect_dbt_model|lookup_snowflake_metadata|build_chart", "answer"?: string, "sql"?: string, "modelName"?: string, "metadataLookup"?: { "kind": "schemas|tables|columns", "database"?: string, "schema"?: string, "table"?: string, "search"?: string }, "chartRequest"?: { "type"?: "bar|line|pie|doughnut", "title"?: string, "xKey"?: string, "yKey"?: string, "seriesKey"?: string, "maxPoints"?: number }, "reasoning"?: string }',
          "",
          `Max query rows per profile: ${profile.maxRowsPerQuery}.`
        ].join("\n")
      },
      {
        role: "system",
        content: [
          "Warehouse context:",
          `- current_date_utc: ${currentDate}`,
          `- current_datetime_utc: ${currentDateIso}`,
          `- tenantId: ${context.tenantId}`,
          `- database: ${snowflakeDatabase || "(not set)"}`,
          `- schema: ${snowflakeSchema || "(not set)"}`,
          `- schema_candidates: ${schemaCandidates.join(", ")}`,
          `- dbt subpath: ${tenantRepo?.dbtSubpath ?? "(unknown)"}`,
          hasWarehouseDefaults
            ? `- example fully-qualified relation: ${fqPrefix}.${quoteSqlIdent("fct_transactions")}`
            : "- fully-qualified relation prefix could not be derived from env."
        ].join("\n")
      },
      {
        role: "system",
        content: `dbt models currently available (name -> path, suggested relation):\n${dbtModels
          .slice(0, 300)
          .map((m) => {
            if (!hasWarehouseDefaults) {
              return `${m.name} -> ${m.relativePath}`;
            }
            const hintedSchema = inferSchemaHintFromModelPath(m.relativePath, snowflakeSchema.toUpperCase());
            return `${m.name} -> ${m.relativePath} -> "${snowflakeDatabase}"."${hintedSchema}".${quoteSqlIdent(m.name)}`;
          })
          .join("\n")}`
      },
      {
        role: "system",
        content:
          toolCalls.length === 0
            ? "Tool call history: none yet."
            : `Tool call history (full context, including failures):\n${asJsonBlock(toolCalls)}`
      },
      ...historyMessages,
      {
        role: "user",
        content: userText
      }
    ];
    let finalPlan: z.infer<typeof plannerSchema> | undefined;
    let finalSql: string | undefined;
    let lastSuccessfulQuery: { sql: string; result: QueryResult } | undefined;
    let latestChartArtifact: AgentArtifact | undefined;

    for (let step = 1; step <= maxPlannerSteps; step += 1) {
      const planRaw = await measure(`plannerMs_step${step}`, async () =>
        this.llm.generateText({
          model: llmModel,
          messages: planningMessages(),
          temperature: 0
        })
      );

      let plan: z.infer<typeof plannerSchema>;
      try {
        plan = plannerSchema.parse(JSON.parse(planRaw));
        plannerAttempts.push({ step, raw: planRaw, plan: plan as Record<string, unknown> });
      } catch (error) {
        plannerAttempts.push({ step, raw: planRaw, parseError: (error as Error).message });
        continue;
      }
      finalPlan = plan;

      if (plan.action === "answer") {
        const text = plan.answer?.trim() ? plan.answer : "I need more details to answer that.";
        this.store.addMessage({
          tenantId: context.tenantId,
          conversationId: context.conversationId,
          role: "assistant",
          content: text
        });
        return {
          text,
          artifacts: latestChartArtifact ? [latestChartArtifact] : undefined,
          debug: {
            plan,
            plannerAttempts,
            sql: finalSql,
            toolCalls,
            timings: { ...timings, totalMs: Date.now() - startedAt }
          }
        };
      }

      if (plan.action === "inspect_dbt_model") {
        const modelName = plan.modelName?.trim();
        if (!modelName) {
          toolCalls.push({
            tool: "dbt.getModelSql",
            input: { tenantId: context.tenantId, modelName: null },
            status: "error",
            durationMs: 0,
            error: "Planner selected inspect_dbt_model without modelName."
          });
          continue;
        }
        const modelSql = await measure("getModelSqlMs", async () =>
          runTool(
            "dbt.getModelSql",
            { tenantId: context.tenantId, modelName },
            async () => this.dbtRepo.getModelSql(context.tenantId, modelName),
            (sqlText) => ({ found: Boolean(sqlText), modelName }),
            (sqlText) => ({ modelName, sql: sqlText })
          )
        );
        if (!modelSql) {
          toolCalls.push({
            tool: "dbt.getModelSql",
            input: { tenantId: context.tenantId, modelName },
            status: "error",
            durationMs: 0,
            error: `Model "${modelName}" was not found in configured dbt repo.`
          });
          continue;
        }
        continue;
      }

      if (plan.action === "lookup_snowflake_metadata") {
        const parsedLookup = metadataLookupSchema.safeParse(plan.metadataLookup);
        if (!parsedLookup.success) {
          toolCalls.push({
            tool: "snowflake.lookupMetadata",
            input: { metadataLookup: plan.metadataLookup ?? null },
            status: "error",
            durationMs: 0,
            error: "Planner selected lookup_snowflake_metadata without valid metadataLookup payload."
          });
          continue;
        }

        const metadataSql = buildMetadataLookupSql(
          parsedLookup.data,
          snowflakeDatabase,
          snowflakeSchema,
          profile.maxRowsPerQuery
        );
        if (!metadataSql) {
          toolCalls.push({
            tool: "snowflake.lookupMetadata",
            input: parsedLookup.data as unknown as Record<string, unknown>,
            status: "error",
            durationMs: 0,
            error: "Metadata lookup requires a database (from env or metadataLookup.database)."
          });
          continue;
        }

        try {
          await runTool(
            "snowflake.lookupMetadata",
            { ...parsedLookup.data, sql: metadataSql },
            async () => this.warehouse.query(metadataSql),
            (result) => ({ rowCount: result.rowCount, columns: result.columns }),
            (result) => ({
              columns: result.columns,
              rowCount: result.rowCount,
              rows: result.rows.slice(0, profile.maxRowsPerQuery)
            })
          );
        } catch {
          // Keep iterating with full failed metadata lookup context.
        }
        continue;
      }

      if (plan.action === "build_chart") {
        const parsedRequest = chartRequestSchema.safeParse(plan.chartRequest ?? {});
        if (!parsedRequest.success) {
          toolCalls.push({
            tool: "chartjs.build",
            input: { chartRequest: plan.chartRequest ?? null },
            status: "error",
            durationMs: 0,
            error: "Planner selected build_chart with invalid chartRequest payload."
          });
          continue;
        }
        if (!lastSuccessfulQuery) {
          toolCalls.push({
            tool: "chartjs.build",
            input: { chartRequest: parsedRequest.data as ChartBuildRequest },
            status: "error",
            durationMs: 0,
            error: "No successful query result available yet. Run query_snowflake first."
          });
          continue;
        }
        const successfulQuery = lastSuccessfulQuery;

        try {
          const chartBuild = await runTool(
            "chartjs.build",
            {
              chartRequest: parsedRequest.data as ChartBuildRequest,
              sourceSql: successfulQuery.sql
            },
            async () =>
              this.chartTool.buildFromQueryResult({
                request: parsedRequest.data as ChartBuildRequest,
                result: successfulQuery.result,
                maxPoints: profile.maxRowsPerQuery
              }),
            (result) => result.summary,
            (result) => ({ config: result.config, summary: result.summary })
          );
          latestChartArtifact = {
            type: "chartjs_config",
            format: "json",
            payload: chartBuild.config,
            summary: chartBuild.summary
          };
        } catch {
          // Keep iterating with full failed chart-build context.
        }
        continue;
      }

      const sql = plan.sql?.trim();
      if (!sql) {
        toolCalls.push({
          tool: "snowflake.query",
          input: { sql: null },
          status: "error",
          durationMs: 0,
          error: "Planner selected query_snowflake without sql."
        });
        continue;
      }

      const normalizedSql = this.sqlGuard.normalize(sql).replace(/\blimit\s+\d+\b/i, `LIMIT ${profile.maxRowsPerQuery}`);
      if (attemptedSql.has(normalizedSql)) {
        toolCalls.push({
          tool: "snowflake.query",
          input: { sql: normalizedSql },
          status: "error",
          durationMs: 0,
          error: "Duplicate SQL attempt in this turn. Generate a different query."
        });
        continue;
      }
      attemptedSql.add(normalizedSql);
      finalSql = normalizedSql;

      try {
        const queryResult = await measure("snowflakeMs", async () =>
          runTool(
            "snowflake.query",
            { sql: normalizedSql },
            async () => this.warehouse.query(normalizedSql),
            (result) => ({ rowCount: result.rowCount, columns: result.columns }),
            (result) => ({
              columns: result.columns,
              rowCount: result.rowCount,
              rows: result.rows.slice(0, profile.maxRowsPerQuery)
            })
          )
        );
        lastSuccessfulQuery = { sql: normalizedSql, result: queryResult };
      } catch {
        // Keep iterating; planner will receive full failed tool context and can retry with corrected SQL.
      }
    }

    if (lastSuccessfulQuery) {
      let text = "";
      try {
        text = await this.llm.generateText({
          model: llmModel,
          temperature: 0.1,
          messages: [
            {
              role: "system",
              content: `${profile.soulPrompt}\nAnswer using business language and include caveats when sample size or nulls matter.`
            },
            {
              role: "user",
              content: [
                `User question: ${userText}`,
                `Executed SQL:\n${lastSuccessfulQuery.sql}`,
                "Result JSON:",
                asJsonBlock({
                  columns: lastSuccessfulQuery.result.columns,
                  rowCount: lastSuccessfulQuery.result.rowCount,
                  rows: lastSuccessfulQuery.result.rows.slice(0, profile.maxRowsPerQuery)
                })
              ].join("\n\n")
            }
          ]
        });
      } catch {
        text = `I successfully executed the query but could not fully synthesize the final narrative. Raw result: ${asJsonBlock(
          {
            columns: lastSuccessfulQuery.result.columns,
            rowCount: lastSuccessfulQuery.result.rowCount,
            rows: lastSuccessfulQuery.result.rows.slice(0, profile.maxRowsPerQuery)
          }
        )}`;
      }

      this.store.addMessage({
        tenantId: context.tenantId,
        conversationId: context.conversationId,
        role: "assistant",
        content: text
      });
      return {
        text,
        artifacts: latestChartArtifact ? [latestChartArtifact] : undefined,
        debug: {
          plan: finalPlan,
          plannerAttempts,
          sql: lastSuccessfulQuery.sql,
          toolCalls,
          timings: { ...timings, totalMs: Date.now() - startedAt },
          finalizedFromLastSuccessfulQuery: true
        }
      };
    }

    const fallback = "I could not reach a reliable final answer after multiple tool attempts. Please try rephrasing.";
    this.store.addMessage({
      tenantId: context.tenantId,
      conversationId: context.conversationId,
      role: "assistant",
      content: fallback
    });
    return {
      text: fallback,
      artifacts: latestChartArtifact ? [latestChartArtifact] : undefined,
      debug: {
        plan: finalPlan,
        plannerAttempts,
        sql: finalSql,
        toolCalls,
        timings: { ...timings, totalMs: Date.now() - startedAt }
      }
    };
  }
}
