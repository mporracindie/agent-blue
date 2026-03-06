import { App } from "@slack/bolt";
import { ChartJSNodeCanvas } from "chartjs-node-canvas";
import type { ChartConfiguration } from "chart.js";
import { AnalyticsAgentRuntime } from "../../../core/agentRuntime.js";

export interface SlackAgentServerOptions {
  runtime: AnalyticsAgentRuntime;
  botToken: string;
  signingSecret: string;
  port: number;
  defaultTenantId?: string;
  defaultProfileName?: string;
  llmModel?: string;
  teamTenantMap?: Record<string, string>;
}

function parseMessageText(raw: string): string {
  return raw.replace(/<@[^>]+>/g, "").trim();
}

function getTeamId(body: unknown, event: Record<string, unknown>): string | null {
  const bodyObj = body as Record<string, unknown>;
  const bodyTeamId = bodyObj["team_id"];
  if (typeof bodyTeamId === "string" && bodyTeamId.length > 0) {
    return bodyTeamId;
  }
  const eventTeamId = event["team"];
  if (typeof eventTeamId === "string" && eventTeamId.length > 0) {
    return eventTeamId;
  }
  return null;
}

function resolveTenantId(
  teamId: string | null,
  defaultTenantId: string | undefined,
  teamTenantMap: Record<string, string> | undefined
): string | null {
  if (teamId && teamTenantMap?.[teamId]) {
    return teamTenantMap[teamId];
  }
  if (defaultTenantId && defaultTenantId.length > 0) {
    return defaultTenantId;
  }
  return null;
}

function buildConversationId(teamId: string, channelId: string, threadTs: string): string {
  const safeThread = threadTs.replace(/\./g, "_");
  return `slack_${teamId}_${channelId}_${safeThread}`;
}

function normalizeThreadText(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const cleaned = value.replace(/\s+/g, " ").trim();
  return cleaned.length > 0 ? cleaned : null;
}

function buildThreadContextPrompt(lines: string[], userMessage: string): string {
  if (lines.length === 0) {
    return userMessage;
  }
  return [
    "Slack thread context (last 10 messages before the current one):",
    ...lines.map((line) => `- ${line}`),
    "",
    `Current message: ${userMessage}`
  ].join("\n");
}

function buildSlackFormattingPrompt(userMessage: string): string {
  return [
    "Formatting rules for this response:",
    "- You are replying in Slack mrkdwn.",
    "- Use *single asterisks* for bold (not **double asterisks**).",
    "- Keep formatting simple: short paragraphs and plain bullet lists.",
    "- Do not use Markdown headings or tables.",
    "",
    `User request: ${userMessage}`
  ].join("\n");
}

interface SlackChartArtifact {
  type?: unknown;
  format?: unknown;
  payload?: unknown;
}

function getChartConfigFromArtifacts(artifacts: unknown): Record<string, unknown> | null {
  if (!Array.isArray(artifacts)) {
    return null;
  }
  for (const artifactRaw of artifacts) {
    const artifact = artifactRaw as SlackChartArtifact;
    if (artifact.type !== "chartjs_config" || artifact.format !== "json") {
      continue;
    }
    if (!artifact.payload || typeof artifact.payload !== "object" || Array.isArray(artifact.payload)) {
      continue;
    }
    return artifact.payload as Record<string, unknown>;
  }
  return null;
}

async function buildChartPngBuffer(config: Record<string, unknown>): Promise<Buffer> {
  const renderer = new ChartJSNodeCanvas({
    width: 900,
    height: 500,
    backgroundColour: "white"
  });
  return renderer.renderToBuffer(config as unknown as ChartConfiguration);
}

export function parseSlackTeamTenantMap(raw: string): Record<string, string> {
  if (!raw.trim()) {
    return {};
  }
  const parsed = JSON.parse(raw) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("SLACK_TEAM_TENANT_MAP must be a JSON object mapping team_id to tenant_id.");
  }
  return Object.fromEntries(
    Object.entries(parsed).filter(
      (entry): entry is [string, string] => typeof entry[0] === "string" && typeof entry[1] === "string"
    )
  );
}

export async function startSlackAgentServer(options: SlackAgentServerOptions): Promise<void> {
  if (!options.botToken) {
    throw new Error("SLACK_BOT_TOKEN is required.");
  }
  if (!options.signingSecret) {
    throw new Error("SLACK_SIGNING_SECRET is required.");
  }

  const app = new App({
    token: options.botToken,
    signingSecret: options.signingSecret
  });

  const processMessage = async (input: {
    teamId: string | null;
    channel: string;
    threadTs: string;
    text: string;
    currentTs: string;
    includeThreadContext: boolean;
    client: App["client"];
  }): Promise<void> => {
    const processingReaction = "hourglass_flowing_sand";
    let reactionAdded = false;
    const tenantId = resolveTenantId(input.teamId, options.defaultTenantId, options.teamTenantMap);
    if (!tenantId) {
      await input.client.chat.postMessage({
        channel: input.channel,
        thread_ts: input.threadTs,
        text: "No tenant mapping found for this Slack workspace. Configure SLACK_DEFAULT_TENANT_ID or SLACK_TEAM_TENANT_MAP."
      });
      return;
    }

    try {
      await input.client.reactions.add({
        channel: input.channel,
        timestamp: input.currentTs,
        name: processingReaction
      });
      reactionAdded = true;
    } catch (error) {
      process.stderr.write(`Warning: failed to add processing reaction: ${(error as Error).message}\n`);
    }

    const profileName = options.defaultProfileName ?? "default";
    const conversationId = buildConversationId(input.teamId ?? "unknown_team", input.channel, input.threadTs);

    try {
      let promptText = buildSlackFormattingPrompt(input.text);
      if (input.includeThreadContext) {
        try {
          const replies = await input.client.conversations.replies({
            channel: input.channel,
            ts: input.threadTs,
            limit: 15,
            inclusive: true
          });
          const messages = Array.isArray(replies.messages) ? replies.messages : [];
          const previousMessages = messages
            .filter((message) => {
              const ts = typeof message.ts === "string" ? Number.parseFloat(message.ts) : Number.NaN;
              const currentTs = Number.parseFloat(input.currentTs);
              return Number.isFinite(ts) && Number.isFinite(currentTs) && ts < currentTs;
            })
            .slice(-10)
            .map((message) => {
              const author =
                typeof message.user === "string"
                  ? `user:${message.user}`
                  : typeof message.bot_id === "string"
                    ? `bot:${message.bot_id}`
                    : "unknown";
              const text = normalizeThreadText(message.text) ?? "(no text)";
              return `${author}: ${text}`;
            });
          promptText = buildSlackFormattingPrompt(buildThreadContextPrompt(previousMessages, input.text));
        } catch (error) {
          process.stderr.write(
            `Warning: failed to read Slack thread context: ${(error as Error).message}\n`
          );
        }
      }

      const response = await options.runtime.respond(
        {
          tenantId,
          profileName,
          conversationId,
          llmModel: options.llmModel
        },
        promptText
      );

      await input.client.chat.postMessage({
        channel: input.channel,
        thread_ts: input.threadTs,
        text: response.text
      });

      const chartConfig = getChartConfigFromArtifacts(response.artifacts);
      if (chartConfig) {
        try {
          const chartPng = await buildChartPngBuffer(chartConfig);
          await input.client.files.uploadV2({
            channel_id: input.channel,
            thread_ts: input.threadTs,
            filename: `chart-${Date.now()}.png`,
            title: "Generated chart",
            file: chartPng
          });
        } catch (chartError) {
          process.stderr.write(`Warning: failed to render/send chart image: ${(chartError as Error).message}\n`);
        }
      }
    } catch (error) {
      await input.client.chat.postMessage({
        channel: input.channel,
        thread_ts: input.threadTs,
        text: `I hit an error while processing that request: ${(error as Error).message}`
      });
    } finally {
      if (reactionAdded) {
        try {
          await input.client.reactions.remove({
            channel: input.channel,
            timestamp: input.currentTs,
            name: processingReaction
          });
        } catch (error) {
          process.stderr.write(`Warning: failed to remove processing reaction: ${(error as Error).message}\n`);
        }
      }
    }
  };

  app.event("app_mention", async ({ event, body, client }) => {
    const slackEvent = event as unknown as Record<string, unknown>;
    const channel = slackEvent["channel"];
    const ts = slackEvent["ts"];
    const threadTs = slackEvent["thread_ts"];
    const text = slackEvent["text"];
    if (typeof channel !== "string" || typeof ts !== "string" || typeof text !== "string") {
      return;
    }

    void processMessage({
      teamId: getTeamId(body, slackEvent),
      channel,
      threadTs: typeof threadTs === "string" ? threadTs : ts,
      text: parseMessageText(text),
      currentTs: ts,
      includeThreadContext: typeof threadTs === "string" && threadTs.length > 0,
      client
    });
  });

  app.message(async ({ message, body, client }) => {
    const slackMessage = message as unknown as Record<string, unknown>;
    if (typeof slackMessage["subtype"] === "string" || typeof slackMessage["bot_id"] === "string") {
      return;
    }
    if (slackMessage["channel_type"] !== "im") {
      return;
    }

    const channel = slackMessage["channel"];
    const ts = slackMessage["ts"];
    const threadTs = slackMessage["thread_ts"];
    const text = slackMessage["text"];
    if (typeof channel !== "string" || typeof ts !== "string" || typeof text !== "string") {
      return;
    }

    void processMessage({
      teamId: getTeamId(body, slackMessage),
      channel,
      threadTs: typeof threadTs === "string" ? threadTs : ts,
      text: text.trim(),
      currentTs: ts,
      includeThreadContext: false,
      client
    });
  });

  await app.start(options.port);
  process.stdout.write(`Slack agent server running on port ${options.port}\n`);
}
