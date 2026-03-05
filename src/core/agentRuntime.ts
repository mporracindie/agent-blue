import { z } from "zod";
import { ConversationStore, DbtRepositoryService, LlmMessage, LlmProvider, WarehouseAdapter } from "./interfaces.js";
import { AgentContext, AgentResponse } from "./types.js";
import { SqlGuard } from "./sqlGuard.js";

const plannerSchema = z.object({
  action: z.enum(["answer", "query_snowflake", "inspect_dbt_model"]),
  answer: z.string().optional(),
  sql: z.string().optional(),
  modelName: z.string().optional(),
  reasoning: z.string().optional()
});

function asJsonBlock(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export class AnalyticsAgentRuntime {
  constructor(
    private readonly llm: LlmProvider,
    private readonly warehouse: WarehouseAdapter,
    private readonly dbtRepo: DbtRepositoryService,
    private readonly store: ConversationStore,
    private readonly sqlGuard: SqlGuard
  ) {}

  async respond(context: AgentContext, userText: string): Promise<AgentResponse> {
    this.store.createConversation(context);
    this.store.addMessage({
      tenantId: context.tenantId,
      conversationId: context.conversationId,
      role: "user",
      content: userText
    });

    const profile = this.store.getOrCreateProfile(context.tenantId, context.profileName);
    const history = this.store.getMessages(context.conversationId, 12);
    const dbtModels = await this.dbtRepo.listModels(context.tenantId).catch(() => []);

    const historyMessages: LlmMessage[] = history
      .filter((m) => m.role !== "tool" && m.role !== "system")
      .map((m): LlmMessage => ({
        role: m.role === "user" ? "user" : "assistant",
        content: m.content
      }));

    const planningMessages: LlmMessage[] = [
      {
        role: "system",
        content: [
          profile.soulPrompt,
          "",
          "You are an analytics orchestrator. Decide if you can directly answer the user,",
          "need to query Snowflake, or inspect a dbt model file.",
          "",
          "Return ONLY valid JSON with fields:",
          '{ "action": "answer|query_snowflake|inspect_dbt_model", "answer"?: string, "sql"?: string, "modelName"?: string, "reasoning"?: string }',
          "",
          `Max query rows per profile: ${profile.maxRowsPerQuery}.`
        ].join("\n")
      },
      {
        role: "system",
        content: `dbt models currently available (name -> path):\n${dbtModels
          .slice(0, 300)
          .map((m) => `${m.name} -> ${m.relativePath}`)
          .join("\n")}`
      },
      ...historyMessages,
      {
        role: "user",
        content: userText
      }
    ];

    const planRaw = await this.llm.generateText({
      model: process.env.LLM_MODEL ?? "openai/gpt-4o-mini",
      messages: planningMessages,
      temperature: 0
    });

    let plan: z.infer<typeof plannerSchema>;
    try {
      plan = plannerSchema.parse(JSON.parse(planRaw));
    } catch {
      const fallback = "I could not parse the planner output. Please rephrase your request.";
      this.store.addMessage({
        tenantId: context.tenantId,
        conversationId: context.conversationId,
        role: "assistant",
        content: fallback
      });
      return { text: fallback, debug: { plannerRaw: planRaw } };
    }

    if (plan.action === "answer") {
      const text = plan.answer ?? "I need more details to answer that.";
      this.store.addMessage({
        tenantId: context.tenantId,
        conversationId: context.conversationId,
        role: "assistant",
        content: text
      });
      return { text, debug: { plan } };
    }

    if (plan.action === "inspect_dbt_model") {
      const modelName = plan.modelName?.trim();
      if (!modelName) {
        const text = "I need a dbt model name to inspect.";
        this.store.addMessage({
          tenantId: context.tenantId,
          conversationId: context.conversationId,
          role: "assistant",
          content: text
        });
        return { text, debug: { plan } };
      }

      const modelSql = await this.dbtRepo.getModelSql(context.tenantId, modelName);
      if (!modelSql) {
        const text = `I could not find model "${modelName}" in the configured dbt repository.`;
        this.store.addMessage({
          tenantId: context.tenantId,
          conversationId: context.conversationId,
          role: "assistant",
          content: text
        });
        return { text, debug: { plan } };
      }

      const explanation = await this.llm.generateText({
        model: process.env.LLM_MODEL ?? "openai/gpt-4o-mini",
        temperature: 0.2,
        messages: [
          {
            role: "system",
            content: `${profile.soulPrompt}\nExplain dbt SQL clearly and identify metrics/grains/filters that matter for business questions.`
          },
          {
            role: "user",
            content: [
              `User question: ${userText}`,
              `dbt model: ${modelName}`,
              "",
              "SQL:",
              modelSql.slice(0, 20_000)
            ].join("\n")
          }
        ]
      });

      this.store.addMessage({
        tenantId: context.tenantId,
        conversationId: context.conversationId,
        role: "assistant",
        content: explanation
      });
      return { text: explanation, debug: { plan, modelName } };
    }

    const sql = plan.sql?.trim();
    if (!sql) {
      const text = "I planned to run Snowflake SQL but no SQL query was provided.";
      this.store.addMessage({
        tenantId: context.tenantId,
        conversationId: context.conversationId,
        role: "assistant",
        content: text
      });
      return { text, debug: { plan } };
    }

    const normalizedSql = this.sqlGuard.normalize(sql).replace(/\blimit\s+\d+\b/i, `LIMIT ${profile.maxRowsPerQuery}`);
    const queryResult = await this.warehouse.query(normalizedSql);

    const answer = await this.llm.generateText({
      model: process.env.LLM_MODEL ?? "openai/gpt-4o-mini",
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
            `Executed SQL:\n${normalizedSql}`,
            "Result JSON:",
            asJsonBlock({
              columns: queryResult.columns,
              rowCount: queryResult.rowCount,
              rows: queryResult.rows.slice(0, profile.maxRowsPerQuery)
            })
          ].join("\n\n")
        }
      ]
    });

    this.store.addMessage({
      tenantId: context.tenantId,
      conversationId: context.conversationId,
      role: "assistant",
      content: answer
    });

    return {
      text: answer,
      debug: {
        plan,
        sql: normalizedSql,
        rowCount: queryResult.rowCount
      }
    };
  }
}
