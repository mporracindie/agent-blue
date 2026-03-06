import { AgentContext, AgentProfile, ConversationMessage, DbtModelInfo, QueryResult } from "./types.js";

export interface LlmMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

export interface LlmProvider {
  generateText(input: { model: string; messages: LlmMessage[]; temperature?: number }): Promise<string>;
}

export interface WarehouseAdapter {
  query(sql: string, opts?: { timeoutMs?: number }): Promise<QueryResult>;
}

export interface ChartBuildRequest {
  type?: "bar" | "line" | "pie" | "doughnut";
  title?: string;
  xKey?: string;
  yKey?: string;
  seriesKey?: string;
  maxPoints?: number;
}

export interface ChartBuildResult {
  config: Record<string, unknown>;
  summary: {
    type: string;
    xKey: string | null;
    yKey: string | null;
    seriesKey: string | null;
    labelsCount: number;
    datasetsCount: number;
    pointsCount: number;
  };
}

export interface ChartTool {
  buildFromQueryResult(input: {
    request: ChartBuildRequest;
    result: QueryResult;
    maxPoints: number;
  }): ChartBuildResult;
}

export interface DbtRepositoryService {
  syncRepo(tenantId: string): Promise<void>;
  listModels(tenantId: string, dbtSubpath?: string): Promise<DbtModelInfo[]>;
  getModelSql(tenantId: string, modelName: string, dbtSubpath?: string): Promise<string | null>;
}

export interface ConversationStore {
  init(): void;
  createConversation(context: AgentContext): void;
  addMessage(message: Omit<ConversationMessage, "id" | "createdAt">): ConversationMessage;
  getMessages(conversationId: string, limit?: number): ConversationMessage[];
  getOrCreateProfile(tenantId: string, profileName: string): AgentProfile;
  upsertTenantRepo(input: {
    tenantId: string;
    repoUrl: string;
    dbtSubpath: string;
    deployKeyPath: string;
    localPath: string;
  }): void;
  getTenantRepo(tenantId: string): {
    tenantId: string;
    repoUrl: string;
    dbtSubpath: string;
    deployKeyPath: string;
    localPath: string;
  } | null;
}

export interface ChannelAdapter {
  send(text: string): Promise<void>;
}
